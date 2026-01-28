from __future__ import annotations

import argparse
import logging
import os
import time
from typing import Optional

from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

from .io import DIARY_FILENAME, detect_origin
from .qc_engine import QCEngine


def build_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("dq_watchdog")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


class RawFileHandler(FileSystemEventHandler):
    def __init__(self, engine: QCEngine, logger: logging.Logger, mtime_tracker: dict):
        super().__init__()
        self.engine = engine
        self.logger = logger
        self.last_processed_mtime = mtime_tracker

    def on_created(self, event: FileSystemEvent):
        self._handle_event(event)

    def on_moved(self, event: FileSystemEvent):
        self._handle_event(event)

    def on_modified(self, event: FileSystemEvent):
        # Sometimes files are written in place.
        self._handle_event(event)

    def _handle_event(self, event: FileSystemEvent):
        if event.is_directory:
            return
        file_path = event.dest_path if hasattr(event, "dest_path") else event.src_path
        name = os.path.basename(file_path)
        if name.lower() == DIARY_FILENAME.lower():
            return
        if not name.lower().endswith(".csv"):
            return
        if detect_origin(file_path, logger=self.logger) is None:
            return

        # Skip if we already processed this exact file path and it has not changed.
        try:
            mtime = os.path.getmtime(file_path)
        except OSError:
            return
        last_mtime = self.last_processed_mtime.get(file_path)
        if last_mtime is not None and mtime <= last_mtime:
            return

        try:
            output = self.engine.process_file(file_path)
            if output:
                self.logger.info("Processed %s -> %s", file_path, output)
                self.last_processed_mtime[file_path] = mtime
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Failed processing %s: %s", file_path, exc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Data quality check engine")
    parser.add_argument("--input", required=True, help="Input folder (data_raw)")
    parser.add_argument("--output", required=True, help="Output folder (data_out)")
    parser.add_argument("--config", required=True, help="Path to dq_master.yaml")
    parser.add_argument("--once", action="store_true", help="Process existing raw files and exit")
    parser.add_argument("--watch", action="store_true", help="Watch input folder for new raw files")
    return parser.parse_args()


def run_once(engine: QCEngine):
    engine.process_directory_once()


def _process_pending_files(engine: QCEngine, logger: logging.Logger, mtime_tracker: dict):
    from .io import list_raw_files

    for file_path in list_raw_files(engine.input_dir, logger=logger):
        try:
            mtime = os.path.getmtime(file_path)
        except OSError:
            continue
        last_mtime = mtime_tracker.get(file_path)
        if last_mtime is not None and mtime <= last_mtime:
            continue
        output = engine.process_file(file_path)
        if output:
            logger.info("Processed %s -> %s", file_path, output)
            mtime_tracker[file_path] = mtime


def run_watch(engine: QCEngine, input_dir: str, logger: logging.Logger):
    # Initial sweep for any files already present.
    mtime_tracker: dict = {}
    _process_pending_files(engine, logger, mtime_tracker)

    handler = RawFileHandler(engine, logger, mtime_tracker)
    observer = Observer()
    observer.schedule(handler, path=input_dir, recursive=False)
    observer.start()
    logger.info("Started watching %s", input_dir)
    try:
        while True:
            time.sleep(2)
            _process_pending_files(engine, logger, mtime_tracker)
    except KeyboardInterrupt:
        logger.info("Stopping watcher")
        observer.stop()
    observer.join()


def main():
    args = parse_args()
    log_path = os.path.join(args.output, "watchdog.log")
    logger = build_logger(log_path)
    engine = QCEngine(config_path=args.config, input_dir=args.input, output_dir=args.output, logger=logger)

    if args.once:
        run_once(engine)
    if args.watch:
        run_watch(engine, args.input, logger)
    if not args.once and not args.watch:
        # Default behavior: process existing files once.
        run_once(engine)


if __name__ == "__main__":
    main()
