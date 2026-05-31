"""
Safety tests: ensure LIVE_TRADING=false is always enforced
and no real order placement code exists.
"""

import importlib
import os
import sys
import unittest


class TestLiveTradingGuard(unittest.TestCase):

    def setUp(self):
        # Ensure LIVE_TRADING is false for normal tests
        os.environ["LIVE_TRADING"] = "false"

    def test_live_trading_env_is_false_by_default(self):
        """LIVE_TRADING must default to false."""
        val = os.getenv("LIVE_TRADING", "false").strip().lower()
        self.assertEqual(val, "false")

    def test_strategy_raises_if_live_trading_true(self):
        """strategy.py must raise RuntimeError if LIVE_TRADING=true."""
        os.environ["LIVE_TRADING"] = "true"
        # Remove cached module to force re-import with new env
        for mod_name in list(sys.modules.keys()):
            if "strategy" in mod_name:
                del sys.modules[mod_name]
        try:
            with self.assertRaises(RuntimeError) as ctx:
                import src.strategy  # noqa: F401
            self.assertIn("paper", str(ctx.exception).lower())
        finally:
            os.environ["LIVE_TRADING"] = "false"
            for mod_name in list(sys.modules.keys()):
                if "strategy" in mod_name:
                    del sys.modules[mod_name]

    def test_paper_engine_raises_if_live_trading_true(self):
        """paper_engine.py must raise RuntimeError if LIVE_TRADING=true."""
        os.environ["LIVE_TRADING"] = "true"
        for mod_name in list(sys.modules.keys()):
            if "paper_engine" in mod_name:
                del sys.modules[mod_name]
        try:
            with self.assertRaises(RuntimeError) as ctx:
                import src.paper_engine  # noqa: F401
            self.assertIn("paper", str(ctx.exception).lower())
        finally:
            os.environ["LIVE_TRADING"] = "false"
            for mod_name in list(sys.modules.keys()):
                if "paper_engine" in mod_name:
                    del sys.modules[mod_name]

    def test_main_exits_if_live_trading_true(self):
        """main.py must sys.exit if LIVE_TRADING=true at import time."""
        # main.py checks at module level and calls sys.exit(1)
        os.environ["LIVE_TRADING"] = "true"
        for mod_name in list(sys.modules.keys()):
            if mod_name in ("src.main", "main"):
                del sys.modules[mod_name]
        try:
            with self.assertRaises(SystemExit) as ctx:
                import src.main  # noqa: F401
            self.assertEqual(ctx.exception.code, 1)
        finally:
            os.environ["LIVE_TRADING"] = "false"
            for mod_name in list(sys.modules.keys()):
                if mod_name in ("src.main", "main"):
                    del sys.modules[mod_name]

    def test_no_real_order_http_calls_in_paper_engine(self):
        """paper_engine.py must not contain real trading HTTP endpoint calls."""
        import inspect
        import src.paper_engine as pe
        source = inspect.getsource(pe)

        forbidden_patterns = [
            "/api/v1/order",
            "/order",
            "place_order",
            "submit_order",
            "create_order",
            "requests.post",
            "httpx.post",
            "aiohttp.post",
        ]
        for pattern in forbidden_patterns:
            self.assertNotIn(
                pattern, source,
                msg=f"paper_engine.py must not contain '{pattern}'",
            )

    def test_no_real_order_http_calls_in_strategy(self):
        """strategy.py must not contain real trading HTTP endpoint calls."""
        import inspect
        import src.strategy as st
        source = inspect.getsource(st)

        forbidden_patterns = [
            "/api/v1/order",
            "place_order",
            "submit_order",
            "create_order",
            "requests.post",
            "httpx.post",
        ]
        for pattern in forbidden_patterns:
            self.assertNotIn(
                pattern, source,
                msg=f"strategy.py must not contain '{pattern}'",
            )

    def test_paper_engine_never_calls_real_endpoints(self):
        """Verify PaperEngine class methods don't make external HTTP calls."""
        os.environ["LIVE_TRADING"] = "false"
        import src.paper_engine as pe

        # Ensure no httpx/requests in the module's globals
        module_attrs = dir(pe)
        self.assertNotIn("httpx", module_attrs)
        self.assertNotIn("requests", module_attrs)


if __name__ == "__main__":
    unittest.main()
