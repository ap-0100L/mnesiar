# Mnesiar

**TODO: Add description**

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `mnesiar` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:mnesiar, "~> 0.1.0"}
  ]
end
```

## Config example
```elixir
config :mnesiar,
  config: %{
    # :cluster :standalone
    mode: :cluster,
    cookie: :very_secret_cookie,
    leader_node: :"consumer@127.0.0.1",
    creator_node: :"consumer@127.0.0.1",
    # [] or :all
    # Skip create tables, all tables will be remote on these nodes
    remote_access_only_node_name_prefixes: [],
    schema_ram_copies_node_name_prefixes: [],
    schema_disc_copies_node_name_prefixes: [~r/consumer@/iu, ~r/telegram_bot_api@/iu, ~r/rest_api_client@/iu],
    entities: [
      %{
        module: ApiCore.Db.InMemory.Dbo.SecurityToken,
        ram_copies_node_name_prefixes: [~r/rest_api@/iu, ~r/rest_api_client@/iu],
        disc_copies_node_name_prefixes: [~r/consumer@/iu],
        disc_only_copies_node_name_prefixes: [],
        master_nodes: [:"consumer@127.0.0.1"],
        # In seconds
        cached_data_ttl: 300
      },
      %{
        module: ApiCore.Db.InMemory.Dbo.Recipient,
        # [] or :all; if all [] table will be remote, if node in schema_*_copies list but not in table *_copies_node table will be remote too
        ram_copies_node_name_prefixes: [~r/rest_api@/iu, ~r/rest_api_client@/iu],
        disc_copies_node_name_prefixes: [~r/consumer@/iu],
        disc_only_copies_node_name_prefixes: [],
        master_nodes: [:"consumer@127.0.0.1"],
        cached_data_ttl: 3600
      }
    ],
    # In milliseconds
    wait_for_tables_timeout: 60_000,
    # In seconds
    wait_for_start_timeout: 10,
    # In seconds
    wait_for_stop_timeout: 10
  }

config :mnesia,
  # dir: "dir",
  dc_dump_limit: 40,
  dump_log_write_threshold: 50_000,
  dir: './mnesia'
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/mnesiar>.

