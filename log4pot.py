# A honeypot for the Log4Shell vulnerability (CVE-2021-44228)

import json
import re
import socket
import sys
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from threading import Thread
from typing import Any, List, Optional
from uuid import uuid4

from expression_parser import parse
from payloader import process_payloads

from s3 import S3Log

try:
    from azure.storage.blob import BlobServiceClient

    azure_import = True
except ImportError:
    print(
        "Azure dependencies not installed, logging to blob storage not available.",
        file=sys.stderr
    )
    azure_import = False

re_exploit = re.compile("\${.*}")

@dataclass
class Logger:
    logfile: str
    blob_connection_str: Optional[str]
    log_container: Optional[str]
    log_blob: Optional[str]
    s3_bucket: Optional[str]

    def __post_init__(self):
        self.f = open(self.logfile, "a")
        if self.blob_connection_str is not None and 'azure.storage.blob' in sys.modules:
            service_client = BlobServiceClient.from_connection_string(self.blob_connection_str)
            container = service_client.get_container_client(self.log_container)
            blob = container.get_blob_client(self.log_blob)
            blob.exists() or blob.create_append_blob()
            self.blob = blob
        else:
            self.blob = None

        if self.s3_bucket is not None and 'boto3' in sys.modules:
            self.s3_log=S3Log(threshold=4, bucket=self.s3_bucket)
        else:
            self.s3_log=None


    def log(self, logtype: str, message: str, **kwargs):
        d = {
            "type": logtype,
            "timestamp": datetime.utcnow().isoformat(),
            **kwargs,
        }
        j = json.dumps(d) + "\n"
        self.f.write(j)
        self.f.flush()
        if self.blob is not None:
            self.blob.append_block(j)
        if self.s3_log:
            self.s3_log.log(j)

    def log_start(self):
        self.log("start", "Log4Pot started")

    def log_request(self, server_port, client, port, request, headers, uuid):
        self.log("request", "A request was received", correlation_id=str(uuid), server_port=server_port, client=client,
                 port=port, request=request, headers=dict(headers))

    def log_exploit(self, location, payload, uuid):
        self.log("exploit", "Exploit detected", correlation_id=str(uuid), location=location, payload=payload,
                 deobfuscated_payload=parse(payload))

    def log_payload(self, uuid, **kwargs):
        self.log("payload", "Payload downloaded", correlation_id=str(uuid), **kwargs)

    def log_exception(self, e: Exception):
        self.log("exception", "Exception occurred", exception=str(e))

    def log_end(self):
        self.log("end", "Log4Pot stopped")

    def close(self):
        self.log_end()
        self.f.close()


class Log4PotHTTPRequestHandler(BaseHTTPRequestHandler):
    def do(self):
        # If a custom server header is set, overwrite the version_string() function
        if self.server.server_header:
            self.version_string = lambda: self.server.server_header
        self.uuid = uuid4()
        self.send_response(200)
        self.send_header("Content-Type", "text/json")
        self.end_headers()
        self.wfile.write(bytes(f'{{ "status": "ok", "id": "{self.uuid}" }}', "utf-8"))

        self.logger = self.server.logger
        self.logger.log_request(self.server.server_address[1], *self.client_address, self.requestline, self.headers,
                                self.uuid)
        self.find_exploit("request", self.requestline)
        for header, value in self.headers.items():
            self.find_exploit(f"header-{header}", value)

    def find_exploit(self, location: str, content: str) -> bool:
        if (m := re_exploit.search(content)):
            logger.log_exploit(location, m.group(0), self.uuid)

            if self.server.download_payloads:
                try:
                    data = process_payloads(
                        parse(m.group(0)),
                        str(self.uuid),
                        self.server.download_dir,
                        self.server.download_class
                    )
                    self.logger.log_payload(self.uuid, **data)
                except Exception as e:
                    self.logger.log_exception(e)

    def __getattribute__(self, __name: str) -> Any:
        if __name.startswith("do_"):
            return self.do
        else:
            return super().__getattribute__(__name)


class Log4PotHTTPServer(ThreadingHTTPServer):
    def __init__(self, logger: Logger, *args, **kwargs):
        self.logger = logger
        self.server_header = kwargs.pop("server_header", None)
        self.download_payloads = kwargs.pop("download_payloads", False),
        self.download_dir = kwargs.pop("download_dir", None)
        self.download_class = kwargs.pop("download_class", None)
        super().__init__(*args, **kwargs)


class Log4PotServerThread(Thread):
    def __init__(self, logger: Logger, port: int, *args, **kwargs):
        self.port = port
        self.server = Log4PotHTTPServer(
            logger,
            ("", port),
            Log4PotHTTPRequestHandler,
            server_header=kwargs.pop("server_header", None),
            download_payloads=kwargs.pop("download_payloads", False),
            download_dir=kwargs.pop("download_dir", None),
            download_class=kwargs.pop("download_class", None)
        )
        super().__init__(name=f"httpserver-{port}", *args, **kwargs)

    def run(self):
        try:
            self.server.serve_forever()
            self.server.server_close()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.log_exception(e)


class Log4PotArgumentParser(ArgumentParser):
    def convert_arg_line_to_args(self, arg_line: str) -> List[str]:
        return arg_line.split()

if __name__ == '__main__':
    argparser = Log4PotArgumentParser(
        description="A honeypot for the Log4Shell vulnerability (CVE-2021-44228).",
        fromfile_prefix_chars="@",
    )
    argparser.add_argument("--port", "-p", action="append", type=int, help="Listening port")
    argparser.add_argument("--log", "-l", type=str, default="log4pot.log", help="Log file")
    argparser.add_argument("--blob-connection-string", "-b", help="Azure blob storage connection string.")
    argparser.add_argument("--log-container", "-lc", default="logs", help="Azure blob container for logs.")
    argparser.add_argument("--log-blob", "-lb", default=socket.gethostname() + ".log", help="Azure blob for logs.")
    argparser.add_argument("--server-header", type=str, default=None, help="Replace the default server header.")
    argparser.add_argument("--download-payloads", action="store_true", default=False,
                           help="[EXPERIMENTAL] Download http(s) and ldap payloads and log indicators.")
    argparser.add_argument("--download-class", action="store_true", default=False,
                           help="[EXPERIMENTAL] Implement downloading Java Class file referenced by the payload..")
    argparser.add_argument("--download-dir", type=str, help="Set a download directory. If given, payloads are stored "
                                                            "persistently and are not deleted after analysis.")
    argparser.add_argument("--s3_bucket", type=str, help="S3 bucket to upload logs to")

    args = argparser.parse_args()
    if args.port is None:
        print("No port specified!", file=sys.stderr)
        sys.exit(1)
    if not azure_import and args.blob_connection_string is not None:
        print("Azure logging requested but no dependency installed!")
        sys.exit(2)

    if not 'boto3' in sys.modules and args.s3_bucket is not None:
        print("S3 logging request but no dependency (boto3) installed! exiting!")
        sys.exit(2)

    logger = Logger(args.log, args.blob_connection_string, args.log_container, args.log_blob, args.s3_bucket)
    threads = [
        Log4PotServerThread(logger, port, server_header=args.server_header, download_payloads=args.download_payloads,
                            download_dir=args.download_dir, download_class=args.download_class)
        for port in args.port
    ]
    logger.log_start()

    for thread in threads:
        thread.start()
        print(f"Started Log4Pot server on port {thread.port}.")

    for thread in threads:
        thread.join()
        print(f"Stopped Log4Pot server on port {thread.port}.")

    logger.close()
