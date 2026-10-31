"""Allow ``python -m primegrid.client``."""

from primegrid.client.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
