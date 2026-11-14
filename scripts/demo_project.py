"""Generate and validate isolated Docker Compose demo ownership names."""

import re
import secrets
import sys

PROJECT_RE = re.compile(r"wf005-demo-[0-9a-f]{12}\Z")


def new_project_name():
    return "wf005-demo-" + secrets.token_hex(6)


def validate_project_name(value):
    if not isinstance(value, str) or not PROJECT_RE.fullmatch(value):
        raise ValueError("invalid demo project identity")
    return value


def main():
    if len(sys.argv) == 1 or sys.argv[1] == "new":
        print(new_project_name())
    elif sys.argv[1] == "validate" and len(sys.argv) == 3:
        print(validate_project_name(sys.argv[2]))
    else:
        raise SystemExit("usage: demo_project.py [new|validate PROJECT]")


if __name__ == "__main__":
    main()
