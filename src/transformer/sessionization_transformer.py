from typing import Any, Dict, Optional, Iterator, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lag, sum as spark_sum, max as spark_max, min as spark_min,
    unix_timestamp, from_unixtime, window, collect_list, struct, 
    explode, coalesce, lit, row_number, monotonically_increasing_id,
    lead, desc, asc, concat, date_format, hash, abs as spark_abs
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, 
    IntegerType, DoubleType, MapType, ArrayType
)
try:
    from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
except ImportError:
    # Fallback for different Spark versions
    GroupState = None
    GroupStateTimeout = None
from src.transformer.base_transformer import BaseTransformer
from src.common.exceptions import TransformationError
import logging
import time
from dataclasses import dataclass


@dataclass
class SessionState:
    """State object to track user session information across micro-batches."""
    session_id: str
    session_start_time_ms: int
    last_event_time_ms: int
    event_count: int
    session_number: int = 1
    
    def to_dict(self):
        return {
            'session_id': self.session_id,
            'session_start_time_ms': self.session_start_time_ms,
            'last_event_time_ms': self.last_event_time_ms,
            'event_count': self.event_count,
            'session_number': self.session_number
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(**data)


@dataclass
class SessionEvent:
    """Represents a single event with session information."""
    user_id: str
    event_id: str
    page_name: str
    event_timestamp: str
    booking_details: str
    event_details: dict
    event_time_ms: int
    session_id: str = None
    session_start_time_ms: int = None
    session_end_time_ms: int = None
    session_duration_seconds: float = None


class StatefulSessionizationTransformer:
    """
    Stateful sessionization transformer using mapGroupsWithState for proper
    cross-batch session tracking in streaming mode.
    """
    
    def __init__(self, inactivity_timeout_seconds: int = 1800, 
                 max_session_duration_seconds: int = 7200):
        self.inactivity_timeout_seconds = inactivity_timeout_seconds  # 30 minutes
        self.max_session_duration_seconds = max_session_duration_seconds  # 2 hours
    
    def update_session_state(self, user_id: str, events: Iterator, state: GroupState) -> Iterator[SessionEvent]:
        """
        State update function for mapGroupsWithState.
        Tracks sessions across micro-batches and enforces business rules.
        """
        # Convert events iterator to list for processing
        event_list = list(events)
        if not event_list:
            return iter([])
        
        # Sort events by timestamp to ensure proper ordering
        event_list.sort(key=lambda x: x.event_time_ms)
        
        # Get or initialize state
        if state.exists:
            session_state = SessionState.from_dict(state.get)
        else:
            # Initialize state for new user
            first_event = event_list[0]
            session_state = SessionState(
                session_id=f"{user_id}_session_{1}",
                session_start_time_ms=first_event.event_time_ms,
                last_event_time_ms=first_event.event_time_ms,
                event_count=0,
                session_number=1
            )
        
        # Process events and generate sessionized output
        sessionized_events = []
        current_session = session_state
        
        for event in event_list:
            # Check session boundary conditions
            time_since_last = (event.event_time_ms - current_session.last_event_time_ms) / 1000.0
            time_since_start = (event.event_time_ms - current_session.session_start_time_ms) / 1000.0
            
            # Rule 1: 30-minute inactivity timeout
            if time_since_last > self.inactivity_timeout_seconds:
                # Start new session due to inactivity
                current_session.session_number += 1
                current_session = SessionState(
                    session_id=f"{user_id}_session_{current_session.session_number}",
                    session_start_time_ms=event.event_time_ms,
                    last_event_time_ms=event.event_time_ms,
                    event_count=1,
                    session_number=current_session.session_number
                )
            
            # Rule 2: 2-hour maximum session duration
            elif time_since_start > self.max_session_duration_seconds:
                # Start new session due to max duration
                current_session.session_number += 1
                current_session = SessionState(
                    session_id=f"{user_id}_session_{current_session.session_number}",
                    session_start_time_ms=event.event_time_ms,
                    last_event_time_ms=event.event_time_ms,
                    event_count=1,
                    session_number=current_session.session_number
                )
            else:
                # Continue current session
                current_session.last_event_time_ms = event.event_time_ms
                current_session.event_count += 1
            
            # Create sessionized event
            sessionized_event = SessionEvent(
                user_id=event.user_id,
                event_id=event.event_id,
                page_name=event.page_name,
                event_timestamp=event.event_timestamp,
                booking_details=event.booking_details,
                event_details=event.event_details,
                event_time_ms=event.event_time_ms,
                session_id=current_session.session_id,
                session_start_time_ms=current_session.session_start_time_ms,
                session_end_time_ms=current_session.last_event_time_ms,
                session_duration_seconds=(current_session.last_event_time_ms - current_session.session_start_time_ms) / 1000.0
            )
            
            sessionized_events.append(sessionized_event)
        
        # Update state for next micro-batch
        state.update(current_session.to_dict())
        
        # Set timeout for state cleanup (remove inactive users after 2 hours)
        state.setTimeoutDuration(self.max_session_duration_seconds * 1000 + 3600000)  # 2hr + 1hr buffer
        
        return iter(sessionized_events)
    
    def handle_timeout(self, user_id: str, state: GroupState) -> Iterator[SessionEvent]:
        """
        Handle state timeout for inactive users.
        Called when a user has been inactive for the timeout duration.
        """
        if state.exists:
            # Optionally emit a final session end event
            # For now, just remove the state
            state.remove()
        
        return iter([])


class SessionizationTransformer(BaseTransformer):
    """
    Real-time user sessionization transformer for clickstream data.
    
    Sessionization Rules:
    - Session ends after 30 minutes of inactivity
    - Session ends after 2 hours of continuous activity (max session duration)
    - Creates session_id, session_start_time_ms, session_end_time_ms for each user
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Sessionization parameters (configurable)
        self.inactivity_timeout_minutes = config.get('inactivity_timeout_minutes', 30)
        self.max_session_duration_hours = config.get('max_session_duration_hours', 2)
        self.user_id_column = config.get('user_id_column', 'uuid')
        self.timestamp_column = config.get('timestamp_column', 'event_timestamp')
        self.watermark_delay = config.get('watermark_delay', '10 minutes')
        
        # Convert to seconds for calculations
        self.inactivity_timeout_seconds = self.inactivity_timeout_minutes * 60
        self.max_session_duration_seconds = self.max_session_duration_hours * 3600
        
        self.logger.info(f"Sessionization rules: {self.inactivity_timeout_minutes}min inactivity, "
                        f"{self.max_session_duration_hours}hr max duration")
    
    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        try:
            self.logger.info("Starting sessionization transformation")
            
            # First apply JSON parsing if needed (this transformer works after JSON parsing)
            if 'schema' in config and 'value_column' in config:
                df = self._parse_json_data(df, config)
            
            # Validate required columns
            required_columns = [self.user_id_column, self.timestamp_column]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise TransformationError(f"Missing required columns: {missing_columns}")
            
            # Convert timestamp to proper format if it's in string format
            df_with_timestamp = self._prepare_timestamp(df)
            
            # Add watermark for late data handling
            df_with_watermark = df_with_timestamp.withWatermark("event_time", self.watermark_delay)
            
            # Apply pre-processing operations
            if 'operations' in config:
                df_with_watermark = self._apply_operations(df_with_watermark, config['operations'])
            
            # Drop unnecessary columns
            if 'drop_columns' in config:
                columns_to_drop = config['drop_columns']
                existing_columns = df_with_watermark.columns
                cols_to_drop = [col for col in columns_to_drop if col in existing_columns]
                if cols_to_drop:
                    df_with_watermark = df_with_watermark.drop(*cols_to_drop)
                    self.logger.info(f"Dropped columns: {cols_to_drop}")
            
            # Check if this is a streaming DataFrame and use appropriate sessionization
            if df_with_watermark.isStreaming:
                self.logger.info("Using streaming-compatible sessionization")
                sessionized_df = self._sessionize_events_streaming(df_with_watermark)
            else:
                self.logger.info("Using batch sessionization with LAG functions")
                sessionized_df = self._sessionize_events(df_with_watermark)
            
            self.logger.info("Sessionization transformation completed successfully")
            return sessionized_df
            
        except Exception as e:
            self.logger.error(f"Sessionization transformation failed: {e}")
            raise TransformationError(f"Failed to sessionize data: {e}")
    
    def _prepare_timestamp(self, df: DataFrame) -> DataFrame:
        """Convert timestamp column to proper timestamp format."""
        try:
            from pyspark.sql.functions import to_timestamp
            
            # Check if timestamp is in milliseconds (epoch) or string format
            if self.timestamp_column in df.columns:
                # Convert from epoch milliseconds to proper timestamp
                df_with_time = df.withColumn(
                    "event_time",
                    # Handle both string and numeric timestamps
                    when(col(self.timestamp_column).cast("double").isNotNull(),
                         to_timestamp(from_unixtime(col(self.timestamp_column).cast("double") / 1000.0))
                    ).otherwise(
                         col(self.timestamp_column).cast("timestamp")
                    )
                )
                
                # Add event_time_ms for calculations
                df_with_time = df_with_time.withColumn(
                    "event_time_ms",
                    unix_timestamp("event_time") * 1000
                )
                
                return df_with_time
            else:
                raise TransformationError(f"Timestamp column '{self.timestamp_column}' not found")
                
        except Exception as e:
            self.logger.error(f"Failed to prepare timestamp: {e}")
            raise TransformationError(f"Timestamp preparation failed: {e}")
    
    def _sessionize_events(self, df: DataFrame) -> DataFrame:
        """
        Main sessionization logic with proper 2-hour max duration session splitting.
        """
        try:
            # Define window function partitioned by user and ordered by time
            user_window = Window.partitionBy(self.user_id_column).orderBy("event_time_ms")
            
            # Calculate time differences between consecutive events
            df_with_diffs = df.withColumn(
                "prev_event_time_ms",
                lag("event_time_ms").over(user_window)
            ).withColumn(
                "time_diff_seconds",
                (col("event_time_ms") - col("prev_event_time_ms")) / 1000.0
            )
            
            # FIRST PASS: Detect inactivity-based session boundaries
            df_with_inactivity_boundaries = df_with_diffs.withColumn(
                "is_inactivity_boundary",
                when(
                    col("prev_event_time_ms").isNull(), True  # First event for user
                ).when(
                    col("time_diff_seconds") > self.inactivity_timeout_seconds, True  # 30min inactivity
                ).otherwise(False)
            )
            
            # Create initial session markers based on inactivity
            df_with_initial_sessions = df_with_inactivity_boundaries.withColumn(
                "initial_session_marker",
                spark_sum(when(col("is_inactivity_boundary"), 1).otherwise(0)).over(user_window)
            )
            
            # SECOND PASS: Apply max duration rule by splitting long sessions
            df_final = self._apply_max_duration_splitting(df_with_initial_sessions, user_window)
            
            return df_final
            
        except Exception as e:
            self.logger.error(f"Sessionization logic failed: {e}")
            raise TransformationError(f"Failed to apply sessionization logic: {e}")
    
    def _apply_max_duration_splitting(self, df: DataFrame, user_window: Window) -> DataFrame:
        """
        Apply 2-hour max duration rule by properly splitting sessions.
        """
        try:
            # Calculate session start time for each initial session
            initial_session_window = Window.partitionBy(self.user_id_column, "initial_session_marker")
            
            df_with_session_starts = df.withColumn(
                "initial_session_start_ms",
                spark_min("event_time_ms").over(initial_session_window)
            )
            
            # Calculate time since session start for each event
            df_with_time_since_start = df_with_session_starts.withColumn(
                "time_since_session_start_seconds",
                (col("event_time_ms") - col("initial_session_start_ms")) / 1000.0
            )
            
            # Mark duration-based boundaries (events that exceed 2-hour limit)
            df_with_duration_boundaries = df_with_time_since_start.withColumn(
                "exceeds_duration_limit", 
                col("time_since_session_start_seconds") > self.max_session_duration_seconds
            )
            
            # Create sub-session markers for duration splits
            # This splits events that exceed 2 hours into new sessions
            df_with_duration_splits = df_with_duration_boundaries.withColumn(
                "duration_split_marker",
                (col("time_since_session_start_seconds") / self.max_session_duration_seconds).cast("int")
            )
            
            # Combine initial session marker with duration splits for final session ID
            df_with_final_sessions = df_with_duration_splits.withColumn(
                "final_session_marker",
                concat(
                    col("initial_session_marker").cast("string"),
                    lit("_"),
                    col("duration_split_marker").cast("string")
                )
            ).withColumn(
                "session_id",
                concat(col(self.user_id_column), lit("_session_"), col("final_session_marker"))
            )
            
            # Calculate final session statistics
            final_session_window = Window.partitionBy(self.user_id_column, "session_id")
            
            df_with_session_stats = df_with_final_sessions.withColumn(
                "session_start_time_ms",
                spark_min("event_time_ms").over(final_session_window)
            ).withColumn(
                "session_end_time_ms",
                spark_max("event_time_ms").over(final_session_window)
            ).withColumn(
                "session_duration_seconds",
                (col("session_end_time_ms") - col("session_start_time_ms")) / 1000.0
            )
            
            # Add human-readable timestamps
            df_final = df_with_session_stats.withColumn(
                "session_start_time",
                from_unixtime(col("session_start_time_ms") / 1000.0)
            ).withColumn(
                "session_end_time",
                from_unixtime(col("session_end_time_ms") / 1000.0)
            )
            
            # Add partitioning columns for optimized Iceberg storage
            df_final = self._add_partitioning_columns(df_final)
            
            # Clean up intermediate columns
            columns_to_drop = [
                "prev_event_time_ms", "time_diff_seconds", "is_inactivity_boundary",
                "initial_session_marker", "initial_session_start_ms", "time_since_session_start_seconds",
                "exceeds_duration_limit", "duration_split_marker", "final_session_marker"
            ]
            
            for col_name in columns_to_drop:
                if col_name in df_final.columns:
                    df_final = df_final.drop(col_name)
            
            self.logger.info("Applied both 30-minute inactivity and 2-hour max duration rules")
            return df_final
            
        except Exception as e:
            self.logger.error(f"Failed to apply max duration splitting: {e}")
            raise TransformationError(f"Max duration splitting failed: {e}")
    
    def _enforce_max_session_duration(self, df: DataFrame) -> DataFrame:
        """
        Enforce maximum session duration by splitting long sessions.
        """
        try:
            # Check if any session exceeds max duration and split if necessary
            df_with_duration_check = df.withColumn(
                "exceeds_max_duration",
                col("session_duration_seconds") > self.max_session_duration_seconds
            )
            
            # For sessions that exceed max duration, we need to split them
            # This is a simplified approach - in production, you might want more sophisticated splitting
            df_final = df_with_duration_check.withColumn(
                "session_end_time_ms",
                when(
                    col("exceeds_max_duration"),
                    col("session_start_time_ms") + (self.max_session_duration_seconds * 1000)
                ).otherwise(
                    col("session_end_time_ms")
                )
            ).withColumn(
                "session_duration_seconds",
                when(
                    col("exceeds_max_duration"),
                    lit(self.max_session_duration_seconds)
                ).otherwise(
                    col("session_duration_seconds")
                )
            )
            
            return df_final.drop("exceeds_max_duration")
            
        except Exception as e:
            self.logger.error(f"Failed to enforce max session duration: {e}")
            raise TransformationError(f"Max session duration enforcement failed: {e}")
    
    def validate_config(self) -> bool:
        """Validate the sessionization configuration."""
        try:
            if self.inactivity_timeout_minutes <= 0:
                self.logger.error("Inactivity timeout must be positive")
                return False
            
            if self.max_session_duration_hours <= 0:
                self.logger.error("Max session duration must be positive")
                return False
            
            if self.inactivity_timeout_minutes >= (self.max_session_duration_hours * 60):
                self.logger.error("Inactivity timeout should be less than max session duration")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Configuration validation failed: {e}")
            return False


    def _parse_json_data(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """Parse JSON data from Kafka value column."""
        try:
            from pyspark.sql.functions import from_json
            from pyspark.sql.types import StructType, StructField, StringType, MapType
            
            value_column = config.get('value_column', 'value')
            
            if value_column not in df.columns:
                raise TransformationError(f"Value column '{value_column}' not found in DataFrame")
            
            # Build schema from config
            schema_config = config.get('schema', [])
            if schema_config:
                schema = self._build_spark_schema(schema_config)
                
                # Convert binary value to string first if needed
                value_col = col(value_column)
                # Check if value column is binary (from Kafka) and convert to string
                value_str = when(value_col.isNotNull(), value_col.cast("string")).otherwise(lit(None))
                
                # Parse JSON and select all columns
                parsed_df = df.withColumn("parsed_data", from_json(value_str, schema))
                result_df = parsed_df.select("*", "parsed_data.*").drop("parsed_data", value_column)
                
                return result_df
            else:
                return df
                
        except Exception as e:
            self.logger.error(f"JSON parsing failed: {e}")
            raise TransformationError(f"Failed to parse JSON data: {e}")
    
    def _build_spark_schema(self, schema_config: list) -> StructType:
        """Build Spark schema from configuration."""
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, 
            DoubleType, BooleanType, TimestampType, MapType
        )
        
        fields = []
        for field_def in schema_config:
            if isinstance(field_def, dict):
                field_name = field_def.get('name')
                field_type_str = field_def.get('type', 'string')
                nullable = field_def.get('nullable', True)
                
                # Handle nested struct types
                if field_type_str == 'struct' and 'fields' in field_def:
                    nested_fields = []
                    for nested_field in field_def['fields']:
                        nested_name = nested_field.get('name')
                        nested_type = self._get_spark_type(nested_field.get('type', 'string'))
                        nested_nullable = nested_field.get('nullable', True)
                        nested_fields.append(StructField(nested_name, nested_type, nested_nullable))
                    field_type = StructType(nested_fields)
                else:
                    field_type = self._get_spark_type(field_type_str)
                
                if field_name and field_type:
                    fields.append(StructField(field_name, field_type, nullable))
        
        return StructType(fields)
    
    def _get_spark_type(self, type_str: str):
        """Convert string type to Spark type."""
        from pyspark.sql.types import (
            StringType, IntegerType, DoubleType, BooleanType, 
            TimestampType, MapType, ArrayType
        )
        
        type_mapping = {
            'string': StringType(),
            'int': IntegerType(),
            'integer': IntegerType(),
            'double': DoubleType(),
            'float': DoubleType(),
            'boolean': BooleanType(),
            'bool': BooleanType(),
            'timestamp': TimestampType(),
            'map<string,string>': MapType(StringType(), StringType()),
            'array<string>': ArrayType(StringType())
        }
        
        return type_mapping.get(type_str.lower(), StringType())
    
    def _apply_operations(self, df: DataFrame, operations: list) -> DataFrame:
        """Apply filtering and other operations."""
        try:
            for operation in operations:
                if isinstance(operation, dict):
                    if 'filter' in operation:
                        df = df.filter(operation['filter'])
                        self.logger.info(f"Applied filter: {operation['filter']}")
                    elif 'select' in operation:
                        df = df.select(*operation['select'])
                        self.logger.info(f"Applied select: {operation['select']}")
            
            return df
        except Exception as e:
            self.logger.error(f"Failed to apply operations: {e}")
            raise TransformationError(f"Operations application failed: {e}")
    
    def _add_partitioning_columns(self, df: DataFrame) -> DataFrame:
        """Add partitioning columns for optimized Iceberg storage."""
        try:
            from pyspark.sql.functions import date_format, hash, abs as spark_abs, date_format
            
            # Add date-based partition column from session start time
            df_with_date = df.withColumn(
                "session_date",
                date_format(col("session_start_time"), "yyyy-MM-dd")
            )
            
            # Add hash-based bucket column for balanced distribution
            # Using 100 buckets for balanced distribution across users
            df_with_bucket = df_with_date.withColumn(
                "user_hash_bucket",
                (spark_abs(hash(col(self.user_id_column))) % 100).cast("int")
            )
            
            self.logger.info("Added partitioning columns: session_date, user_hash_bucket")
            return df_with_bucket
            
        except Exception as e:
            self.logger.error(f"Failed to add partitioning columns: {e}")
            raise TransformationError(f"Partitioning columns addition failed: {e}")


    def _sessionize_events_streaming(self, df: DataFrame) -> DataFrame:
        """
        Stateful streaming sessionization using mapGroupsWithState for proper
        cross-batch session tracking with 30min inactivity and 2hr max duration rules.
        """
        try:
            from pyspark.sql.streaming import GroupStateTimeout
            from pyspark.sql.functions import struct
            
            self.logger.info("Using stateful streaming sessionization with mapGroupsWithState")
            
            # Create the stateful transformer
            stateful_transformer = StatefulSessionizationTransformer(
                inactivity_timeout_seconds=self.inactivity_timeout_seconds,
                max_session_duration_seconds=self.max_session_duration_seconds
            )
            
            # Define the schema for session events that will be returned
            session_schema = StructType([
                StructField("uuid", StringType(), True),
                StructField("event_id", StringType(), True),
                StructField("page_name", StringType(), True),
                StructField("event_timestamp", StringType(), True),
                StructField("booking_details", StringType(), True),
                StructField("event_details", MapType(StringType(), StringType()), True),
                StructField("event_time_ms", LongType(), True),
                StructField("session_id", StringType(), True),
                StructField("session_start_time_ms", LongType(), True),
                StructField("session_end_time_ms", LongType(), True),
                StructField("session_duration_seconds", DoubleType(), True),
            ])
            
            # Group by user and apply stateful session tracking
            sessionized_stream = df.groupByKey(
                lambda row: row[self.user_id_column]
            ).mapGroupsWithState(
                stateful_transformer.update_session_state,
                session_schema,
                timeout=GroupStateTimeout.ProcessingTimeTimeout
            )
            
            # Convert back to DataFrame and add additional columns
            sessionized_df = sessionized_stream.toDF()
            
            # Add human-readable timestamps
            sessionized_df = sessionized_df.withColumn(
                "session_start_time",
                from_unixtime(col("session_start_time_ms") / 1000.0).cast("timestamp")
            ).withColumn(
                "session_end_time",
                from_unixtime(col("session_end_time_ms") / 1000.0).cast("timestamp")
            ).withColumn(
                "event_time",
                from_unixtime(col("event_time_ms") / 1000.0).cast("timestamp")
            )
            
            # Add partitioning columns for optimized Iceberg storage
            partitioned_df = self._add_partitioning_columns(sessionized_df)
            
            self.logger.info("Applied stateful streaming sessionization with proper session boundaries")
            return partitioned_df
            
        except Exception as e:
            self.logger.error(f"Stateful streaming sessionization failed: {e}")
            # Fallback to simplified streaming approach if stateful fails
            self.logger.warning("Falling back to simplified streaming sessionization")
            return self._sessionize_events_streaming_fallback(df)
    
    def _sessionize_events_streaming_fallback(self, df: DataFrame) -> DataFrame:
        """
        Fallback streaming sessionization using time windows.
        This is a simplified approach that doesn't track state across batches.
        """
        try:
            from pyspark.sql.functions import (
                window, collect_list, struct, 
                explode, posexplode, concat, lit
            )
            
            self.logger.info("Using fallback streaming sessionization with time windows")
            
            # Use time windows to group events (5-minute windows for processing)
            windowed_df = df.groupBy(
                self.user_id_column,
                window(col("event_time"), "5 minutes")
            ).agg(
                collect_list(struct(
                    col("uuid"),
                    col("event_id"), 
                    col("page_name"),
                    col("event_timestamp"),
                    col("booking_details"),
                    col("event_details"),
                    col("event_time"),
                    col("event_time_ms")
                )).alias("events")
            )
            
            # Create simple session IDs based on time windows
            session_df = windowed_df.withColumn(
                "session_id",
                concat(
                    col(self.user_id_column),
                    lit("_session_"),
                    col("window.start").cast("string")
                )
            ).withColumn(
                "session_start_time_ms",
                col("window.start").cast("long") * 1000
            ).withColumn(
                "session_end_time_ms", 
                col("window.end").cast("long") * 1000
            ).withColumn(
                "session_duration_seconds",
                (col("session_end_time_ms") - col("session_start_time_ms")) / 1000.0
            )
            
            # Add readable timestamps
            session_df = session_df.withColumn(
                "session_start_time",
                from_unixtime(col("session_start_time_ms") / 1000.0).cast("timestamp")
            ).withColumn(
                "session_end_time",
                from_unixtime(col("session_end_time_ms") / 1000.0).cast("timestamp")
            )
            
            # Explode events back to individual rows
            exploded_df = session_df.select(
                col("*"),
                posexplode("events").alias("event_pos", "event_data")
            ).select(
                col("event_data.uuid").alias("uuid"),
                col("event_data.event_id").alias("event_id"),
                col("event_data.page_name").alias("page_name"),
                col("event_data.event_timestamp").alias("event_timestamp"),
                col("event_data.booking_details").alias("booking_details"),
                col("event_data.event_details").alias("event_details"),
                col("session_id"),
                col("session_start_time_ms"),
                col("session_end_time_ms"),
                col("session_duration_seconds"),
                col("session_start_time"),
                col("session_end_time"),
                col("event_data.event_time").alias("event_time"),
                col("event_data.event_time_ms").alias("event_time_ms")
            )
            
            # Add partitioning columns
            partitioned_df = self._add_partitioning_columns(exploded_df)
            
            self.logger.info("Applied fallback streaming sessionization with time windows")
            return partitioned_df
            
        except Exception as e:
            self.logger.error(f"Fallback streaming sessionization failed: {e}")
            raise TransformationError(f"Failed to apply streaming sessionization: {e}")


def concat(*cols):
    """Helper function for string concatenation."""
    from pyspark.sql.functions import concat as spark_concat
    return spark_concat(*cols)