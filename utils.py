import base64

class Utils:

    @staticmethod
    def decode_b64_into_text(b64_encoded_data: bytes) -> str:
        """Returns string from base64 encoded bytes input."""
        return base64.b64decode(b64_encoded_data).decode('utf-8')