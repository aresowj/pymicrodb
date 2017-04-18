import uuid


def generate_id():
    # type: () -> int
    return str(uuid.uuid4())
