"""
Remote profiling utilities for Redis benchmark runner.

This module provides functionality to trigger remote profiling of Redis processes
via HTTP GET endpoints during benchmark execution. Profiles are collected in
folded format for performance analysis.
"""

import datetime
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional, Dict, Any
import requests


def extract_redis_pid(redis_conn) -> Optional[int]:
    """
    Extract Redis process ID from Redis INFO command.
    
    Args:
        redis_conn: Redis connection object
        
    Returns:
        Redis process ID as integer, or None if not found
    """
    try:
        redis_info = redis_conn.info()
        pid = redis_info.get("process_id")
        if pid is not None:
            logging.info(f"Extracted Redis PID: {pid}")
            return int(pid)
        else:
            logging.warning("Redis process_id not found in INFO command")
            return None
    except Exception as e:
        logging.error(f"Failed to extract Redis PID: {e}")
        return None


def extract_redis_metadata(redis_conn) -> Dict[str, Any]:
    """
    Extract Redis metadata for profile comments.
    
    Args:
        redis_conn: Redis connection object
        
    Returns:
        Dictionary containing Redis metadata
    """
    try:
        redis_info = redis_conn.info()
        metadata = {
            "redis_version": redis_info.get("redis_version", "unknown"),
            "redis_git_sha1": redis_info.get("redis_git_sha1", "unknown"),
            "redis_git_dirty": redis_info.get("redis_git_dirty", "unknown"),
            "redis_build_id": redis_info.get("redis_build_id", "unknown"),
            "process_id": redis_info.get("process_id", "unknown"),
            "tcp_port": redis_info.get("tcp_port", "unknown"),
        }
        
        # Use build_id if git_sha1 is empty or 0
        if metadata["redis_git_sha1"] in ("", 0, "0"):
            metadata["redis_git_sha1"] = metadata["redis_build_id"]
            
        logging.info(f"Extracted Redis metadata: version={metadata['redis_version']}, sha={metadata['redis_git_sha1']}, pid={metadata['process_id']}")
        return metadata
    except Exception as e:
        logging.error(f"Failed to extract Redis metadata: {e}")
        return {
            "redis_version": "unknown",
            "redis_git_sha1": "unknown", 
            "redis_git_dirty": "unknown",
            "redis_build_id": "unknown",
            "process_id": "unknown",
            "tcp_port": "unknown",
        }


def calculate_profile_duration(benchmark_duration_seconds: int) -> int:
    """
    Calculate profiling duration based on benchmark duration.
    
    Args:
        benchmark_duration_seconds: Expected benchmark duration in seconds
        
    Returns:
        Profiling duration in seconds (minimum: benchmark duration, maximum: 30)
    """
    # Minimum duration is the benchmark duration, maximum is 30 seconds
    duration = min(max(benchmark_duration_seconds, 10), 30)
    logging.info(f"Calculated profile duration: {duration}s (benchmark: {benchmark_duration_seconds}s)")
    return duration


def trigger_remote_profile(
    host: str,
    port: int,
    pid: int,
    duration: int,
    timeout: int = 60,
    username: Optional[str] = None,
    password: Optional[str] = None
) -> Optional[str]:
    """
    Trigger remote profiling via HTTP GET request.

    Args:
        host: Remote host address
        port: Remote port number
        pid: Redis process ID
        duration: Profiling duration in seconds
        timeout: HTTP request timeout in seconds
        username: Optional username for HTTP basic authentication
        password: Optional password for HTTP basic authentication

    Returns:
        Profile content in folded format, or None if failed
    """
    url = f"http://{host}:{port}/debug/folded/profile"
    params = {
        "pid": pid,
        "seconds": duration
    }

    # Prepare authentication if provided
    auth = None
    if username is not None and password is not None:
        auth = (username, password)
        logging.info(f"Using HTTP basic authentication with username: {username}")

    try:
        logging.info(f"Triggering remote profile: {url} with PID={pid}, duration={duration}s")
        response = requests.get(url, params=params, timeout=timeout, auth=auth)
        response.raise_for_status()

        profile_content = response.text
        logging.info(f"Successfully collected profile: {len(profile_content)} characters")
        return profile_content

    except requests.exceptions.Timeout:
        logging.error(f"Remote profiling request timed out after {timeout}s")
        return None
    except requests.exceptions.ConnectionError:
        logging.error(f"Failed to connect to remote profiling endpoint: {host}:{port}")
        return None
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error during remote profiling: {e}")
        if e.response.status_code == 401:
            logging.error("Authentication failed - check username and password")
        return None
    except Exception as e:
        logging.error(f"Unexpected error during remote profiling: {e}")
        return None


def save_profile_with_metadata(
    profile_content: str,
    benchmark_name: str,
    output_dir: str,
    redis_metadata: Dict[str, Any],
    duration: int
) -> Optional[str]:
    """
    Save profile content to file with metadata comments.
    
    Args:
        profile_content: Profile data in folded format
        benchmark_name: Name of the benchmark
        output_dir: Output directory path
        redis_metadata: Redis metadata dictionary
        duration: Profiling duration in seconds
        
    Returns:
        Path to saved file, or None if failed
    """
    try:
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        filename = f"{benchmark_name}.folded"
        filepath = os.path.join(output_dir, filename)
        
        # Generate timestamp
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Create metadata comment
        metadata_comment = (
            f"# profile from redis sha = {redis_metadata['redis_git_sha1']} "
            f"and pid {redis_metadata['process_id']} for duration of {duration}s. "
            f"collection in date {timestamp}\n"
            f"# benchmark_name={benchmark_name}\n"
            f"# redis_git_sha1={redis_metadata['redis_git_sha1']}\n"
            f"# redis_version={redis_metadata['redis_version']}\n"
            f"# redis_git_dirty={redis_metadata['redis_git_dirty']}\n"
        )
        
        # Write file with metadata and profile content
        with open(filepath, 'w') as f:
            f.write(metadata_comment)
            f.write(profile_content)
            
        logging.info(f"Saved profile to: {filepath}")
        return filepath
        
    except Exception as e:
        logging.error(f"Failed to save profile file: {e}")
        return None


class RemoteProfiler:
    """
    Remote profiler class to handle threaded profiling execution.
    """

    def __init__(self, host: str, port: int, output_dir: str, username: Optional[str] = None, password: Optional[str] = None):
        self.host = host
        self.port = port
        self.output_dir = output_dir
        self.username = username
        self.password = password
        self.profile_thread = None
        self.profile_result = None
        self.profile_error = None
        
    def start_profiling(
        self,
        redis_conn,
        benchmark_name: str,
        benchmark_duration_seconds: int
    ) -> bool:
        """
        Start profiling in a separate thread.
        
        Args:
            redis_conn: Redis connection object
            benchmark_name: Name of the benchmark
            benchmark_duration_seconds: Expected benchmark duration
            
        Returns:
            True if profiling thread started successfully, False otherwise
        """
        try:
            # Extract Redis metadata and PID
            redis_metadata = extract_redis_metadata(redis_conn)
            pid = redis_metadata.get("process_id")
            
            if pid == "unknown" or pid is None:
                logging.error("Cannot start remote profiling: Redis PID not available")
                return False
                
            # Calculate profiling duration
            duration = calculate_profile_duration(benchmark_duration_seconds)
            
            # Start profiling thread
            self.profile_thread = threading.Thread(
                target=self._profile_worker,
                args=(pid, duration, benchmark_name, redis_metadata),
                daemon=True
            )
            self.profile_thread.start()
            
            logging.info(f"Started remote profiling thread for benchmark: {benchmark_name}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to start remote profiling: {e}")
            return False
    
    def _profile_worker(self, pid: int, duration: int, benchmark_name: str, redis_metadata: Dict[str, Any]):
        """
        Worker function for profiling thread.
        """
        try:
            # Trigger remote profiling
            profile_content = trigger_remote_profile(
                self.host, self.port, pid, duration,
                username=self.username, password=self.password
            )

            if profile_content:
                # Save profile with metadata
                filepath = save_profile_with_metadata(
                    profile_content, benchmark_name, self.output_dir, redis_metadata, duration
                )
                self.profile_result = filepath
            else:
                self.profile_error = "Failed to collect profile content"

        except Exception as e:
            self.profile_error = f"Profile worker error: {e}"
            logging.error(self.profile_error)
    
    def wait_for_completion(self, timeout: int = 60) -> bool:
        """
        Wait for profiling thread to complete.
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if completed successfully, False if timed out or failed
        """
        if self.profile_thread is None:
            return False
            
        try:
            self.profile_thread.join(timeout=timeout)
            
            if self.profile_thread.is_alive():
                logging.warning(f"Remote profiling thread did not complete within {timeout}s")
                return False
                
            if self.profile_error:
                logging.error(f"Remote profiling failed: {self.profile_error}")
                return False
                
            if self.profile_result:
                logging.info(f"Remote profiling completed successfully: {self.profile_result}")
                return True
            else:
                logging.warning("Remote profiling completed but no result available")
                return False
                
        except Exception as e:
            logging.error(f"Error waiting for remote profiling completion: {e}")
            return False
