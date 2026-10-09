"""Entry point: land raw ``orders`` events into the Bronze MinIO layer."""

from lakehouse.runner import main

if __name__ == "__main__":
    raise SystemExit(main())
