import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime


class LoggerFactory:
    
    _loggers = {}
    _default_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    _default_level = logging.INFO
    
    @classmethod
    def get_logger(cls, name: str, config: Optional[Dict[str, Any]] = None) -> logging.Logger:
        if name in cls._loggers:
            return cls._loggers[name]
        
        logger = logging.getLogger(name)
        
        if config:
            level = config.get('level', cls._default_level)
            if isinstance(level, str):
                level = getattr(logging, level.upper(), cls._default_level)
            logger.setLevel(level)
            
            log_format = config.get('format', cls._default_format)
        else:
            logger.setLevel(cls._default_level)
            log_format = cls._default_format
        
        if not logger.handlers:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter(log_format))
            logger.addHandler(console_handler)
            
            log_dir = Path("logs")
            if log_dir.exists():
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_handler = logging.FileHandler(
                    log_dir / f"{name}_{timestamp}.log"
                )
                file_handler.setFormatter(logging.Formatter(log_format))
                logger.addHandler(file_handler)
        
        cls._loggers[name] = logger
        return logger
    
    @classmethod
    def configure_root_logger(cls, config: Dict[str, Any]) -> None:
        root_logger = logging.getLogger()
        
        level = config.get('level', 'INFO')
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)
        root_logger.setLevel(level)
        
        log_format = config.get('format', cls._default_format)
        
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(log_format))
        root_logger.addHandler(console_handler)
        
        if config.get('file_logging', False):
            log_dir = Path(config.get('log_dir', 'logs'))
            log_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_handler = logging.FileHandler(
                log_dir / f"sessionize_{timestamp}.log"
            )
            file_handler.setFormatter(logging.Formatter(log_format))
            root_logger.addHandler(file_handler)


def setup_logging(config: Optional[Dict[str, Any]] = None) -> None:
    if config:
        LoggerFactory.configure_root_logger(config)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stdout
        )