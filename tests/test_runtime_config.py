import os
import unittest
from unittest.mock import patch

import runtime_config as config


class DatabaseConfigurationTests(unittest.TestCase):
    def test_local_defaults_use_project_socket_and_do_not_force_a_username(self):
        with patch.dict(os.environ, {}, clear=True):
            for name in ("media", "proxy"):
                self.assertEqual(config.database_options(name), {
                    "dbname": name, "connect_timeout": 5,
                    "host": str(config.STATE_DIR / "postgres/socket"),
                })

    def test_standard_postgres_environment_does_not_fall_back_to_mac_socket(self):
        for key in ("PGHOST", "PGHOSTADDR", "PGSERVICE"):
            with self.subTest(key=key), patch.dict(os.environ, {key: "configured"}, clear=True):
                self.assertEqual(config.database_options("proxy"), {"dbname": "proxy", "connect_timeout": 5})

    def test_separate_urls_support_custom_database_user_and_password(self):
        values = {"MEDIA_DATABASE_URL": "host=media-db dbname=videos user=collector password='spaces @ /'",
                  "PROXY_DATABASE_URL": "postgresql://proxy_user:example@proxy-db:6432/catalog"}
        with patch.dict(os.environ, values, clear=True):
            media = config.database_options("media")
            proxy = config.database_options("proxy")
        self.assertEqual((media["host"], media["dbname"], media["user"], media["password"]),
                         ("media-db", "videos", "collector", "spaces @ /"))
        self.assertEqual((proxy["host"], proxy["port"], proxy["dbname"], proxy["user"]),
                         ("proxy-db", "6432", "catalog", "proxy_user"))

    def test_call_options_override_dsn_and_unknown_database_is_rejected(self):
        with patch.dict(os.environ, {"PROXY_DATABASE_URL": "host=db connect_timeout=20"}, clear=True), \
                patch.object(config.psycopg, "connect") as connect:
            config.connect_database("proxy", connect_timeout=2, autocommit=True)
            connect.assert_called_once_with(host="db", dbname="proxy", connect_timeout=2, autocommit=True)
            with self.assertRaises(ValueError):
                config.connect_database("unrelated")
            self.assertEqual(connect.call_count, 1)
