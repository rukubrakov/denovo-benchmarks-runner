"""Display utilities for orchestration system."""


def print_header(text: str):
    """Print a nice header."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")


def print_banner():
    """Print the main banner."""
    print("\n" + "█" * 70)
    print("█" + " " * 68 + "█")
    print("█" + "  Denovo Benchmarks Orchestration System".center(68) + "█")
    print("█" + " " * 68 + "█")
    print("█" * 70 + "\n")


def print_step(text: str):
    """Print a step indicator."""
    print(f"➜ {text}")


def print_success(text: str):
    """Print success message."""
    print(f"  ✓ {text}")


def print_info(text: str):
    """Print info message."""
    print(f"  ℹ {text}")


def print_warning(text: str):
    """Print warning message."""
    print(f"  ⚠ {text}")


def print_error(text: str):
    """Print error message."""
    print(f"  ✗ {text}")
