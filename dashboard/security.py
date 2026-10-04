"""Password verification and signed, query-bound pagination cursors."""
import base64
import hashlib
import hmac
import json
import secrets


def password_hash(password):
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 310000).hex()
    return f'pbkdf2_sha256$310000${salt}${digest}'


def verify_password(password, encoded):
    try:
        algorithm, rounds, salt, expected = encoded.split('$')
        if algorithm != 'pbkdf2_sha256' or not 100000 <= int(rounds) <= 1000000:
            return False
        actual = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), int(rounds)).hex()
        return hmac.compare_digest(actual, expected)
    except (ValueError, TypeError):
        return False


class Cursors:
    def __init__(self, secret):
        self.secret = secret.encode()

    def encode(self, value):
        body = base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':'), default=str).encode()).rstrip(b'=')
        signature = hmac.new(self.secret, body, hashlib.sha256).hexdigest()
        return body.decode() + '.' + signature

    def decode(self, value, scope):
        try:
            if len(value) > 4096:
                raise ValueError()
            body, signature = value.rsplit('.', 1)
            expected = hmac.new(self.secret, body.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError()
            result = json.loads(base64.urlsafe_b64decode(body+'='*(-len(body) % 4)))
            if (not isinstance(result,dict) or result.get('scope') != scope or result.get('direction') not in ('next', 'previous')
                    or type(result.get('page')) is not int or not 1 <= result['page'] <= 1000000
                    or not isinstance(result.get('keys'), list)):
                raise ValueError()
            return result
        except (ValueError, TypeError, KeyError, UnicodeError) as exc:
            raise ValueError('This page link is no longer valid. Return to the first page.') from exc
