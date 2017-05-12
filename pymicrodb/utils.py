import uuid


def generate_id():
    # type: () -> str
    return str(uuid.uuid4())
