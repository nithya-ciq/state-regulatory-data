"""Dagster resource wrapping the Supabase Python client."""

import os
import dagster as dg
from supabase import create_client, Client


class SupabaseResource(dg.ConfigurableResource):
    """Provides a Supabase client to Dagster assets.

    Uses service_role key for server-side operations (bypasses RLS).
    Configure via resource fields or SUPABASE_URL / SUPABASE_KEY env vars.
    """

    supabase_url: str = ""
    supabase_key: str = ""

    def get_client(self) -> Client:
        """Create and return a Supabase client."""
        url = self.supabase_url or os.environ.get("SUPABASE_URL", "")
        key = self.supabase_key or os.environ.get("SUPABASE_KEY", "")
        if not url or not key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_KEY must be set via resource config "
                "or environment variables."
            )
        return create_client(url, key)
