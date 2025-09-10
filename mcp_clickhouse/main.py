# Handle both module and direct script execution
import sys
import os
if __name__ == "__main__":
    # Add parent directory to path for direct script execution
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from mcp_clickhouse.mcp_server import mcp
    from mcp_clickhouse.mcp_env import get_config, TransportType
else:
    # Relative imports for module execution
    from .mcp_server import mcp
    from .mcp_env import get_config, TransportType


def main():
    config = get_config()
    transport = config.mcp_server_transport

    # For HTTP and SSE transports, we need to specify host and port
    http_transports = [TransportType.HTTP.value, TransportType.SSE.value]
    if transport in http_transports:
        # Use the configured bind host (defaults to 127.0.0.1, can be set to 0.0.0.0)
        # and bind port (defaults to 8000)
        mcp.run(transport=transport, host=config.mcp_bind_host, port=config.mcp_bind_port)
    else:
        # For stdio transport, no host or port is needed
        mcp.run(transport=transport)


if __name__ == "__main__":
    main()
