# Mnesiar

**TODO: Add description**

## Installation


```elixir
def deps do
  [
    {:mnesiar, "~> 0.1.0"}
  ]
end
```

```elixir
  ##############################################################################
  @doc """
  # get_children!
  """
  defp get_children! do
    {:ok, config} = Mnesiar.get_config!()

    result = [
      {Mnesiar, [config: config, opts: []]},
    ]

    {:ok, result}
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
    schema_disc_copies_node_name_prefixes: [~r/consumer@/iu, ~r/telegram_bot_api@/iu, ~r/transport@/iu],
    entities: [
      %{
        module: ApiCore.Db.InMemory.Dbo.SecurityToken,
        ram_copies_node_name_prefixes: [~r/rest_api@/iu, ~r/transport@/iu],
        disc_copies_node_name_prefixes: [~r/consumer@/iu],
        disc_only_copies_node_name_prefixes: [],
        master_nodes: [:"consumer@127.0.0.1"],
        # In seconds
        cached_data_ttl: 300
      },
      %{
        module: ApiCore.Db.InMemory.Dbo.Recipient,
        # [] or :all; if all [] table will be remote, if node in schema_*_copies list but not in table *_copies_node table will be remote too
        ram_copies_node_name_prefixes: [~r/rest_api@/iu, ~r/transport@/iu],
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
  dump_log_write_threshold: 50_000
```

### Runtime config

```elixir
import Config

import ConfigUtils, only: [get_env!: 3, get_env!: 2, get_env_name!: 1, in_container!: 0]

{:ok, in_container} = in_container!()

if in_container do
  config :logger,
    handle_otp_reports: true,
    backends: [
      :console
    ]

  config :logger,
         :console,
         level: get_env!("CONSOLE_LOG_LEVEL"), :atom, :info),
         format: get_env!("LOG_FORMAT"), :string, "[$date] [$time] [$level] [$node] [$metadata] [$levelpad] [$message]\n"),
         metadata: :all
else
  config :logger,
    handle_otp_reports: true,
    backends: [
      :console,
      {LoggerFileBackend, :info_log},
      {LoggerFileBackend, :error_log}
    ]

  config :logger,
         :console,
         level: get_env!("CONSOLE_LOG_LEVEL"), :atom, :info),
         format: get_env!("LOG_FORMAT"), :string, "[$date] [$time] [$level] [$node] [$metadata] [$levelpad] [$message]\n"),
         metadata: :all

  config :logger,
         :info_log,
         level: :info,
         path: get_env!("LOG_PATH"), :string, "log") <> "/#{Node.self()}/info.log",
         format: get_env!("LOG_FORMAT"), :string, "[$date] [$time] [$level] [$node] [$metadata] [$levelpad] [$message]\n"),
         metadata: :all

  config :logger,
         :error_log,
         level: :error,
         path: get_env!("LOG_PATH"), :string, "log") <> "/#{Node.self()}/error.log",
         format: get_env!("LOG_FORMAT"), :string, "[$date] [$time] [$level] [$node] [$metadata] [$levelpad] [$message]\n"),
         metadata: :all
end

  config :mnesiar,
    config: %{
      # :cluster :standalone
      mode: get_env!("MNESIAR_SERVER_MODE"), :atom),
      cookie: get_env!("MNESIAR_COOKIE"), :atom),
      leader_node: get_env!("MNESIAR_LEADER_NODE"), :atom),
      creator_node: get_env!("MNESIAR_CREATOR_NODE"), :atom),
      # [] or :all
      # Skip create tables, all tables will be remote on these nodes
      remote_access_only_node_name_prefixes: get_env!("MNESIAR_REMOTE_ACCESS_ONLY_NODE_NAME_PREFIXES"), :list_of_regex, []),
      schema_ram_copies_node_name_prefixes: get_env!("MNESIAR_SCHEMA_RAM_COPIES_NODE_NAME_PREFIXES"), :list_of_regex, []),
      schema_disc_copies_node_name_prefixes: get_env!("MNESIAR_SCHEMA_DISC_COPIES_NODE_NAME_PREFIXES"), :list_of_regex, :all),
      entities: [
        %{
          module: ApiCore.Db.InMemory.Dbo.SecurityToken,
          ram_copies_node_name_prefixes: get_env!("MNESIAR_RAM_COPIES_NODE_NAME_PREFIXES"), :list_of_regex, []),
          disc_copies_node_name_prefixes: get_env!("MNESIAR_DISC_COPIES_NODE_NAME_PREFIXES"), :list_of_regex),
          disc_only_copies_node_name_prefixes: get_env!("MNESIAR_DISC_ONLY_COPIES_NODE_NAME_PREFIXES"), :list_of_regex, []),
          master_nodes: get_env!("MNESIAR_MASTER_NODES"), :list_of_atoms),
          # In seconds
          cached_data_ttl: get_env!("MNESIAR_CACHED_DATA_TTL"), :integer, 300)
        },
        %{
          module: ApiCore.Db.InMemory.Dbo.Essence,
          ram_copies_node_name_prefixes: get_env!("MNESIAR_RAM_COPIES_NODE_NAME_PREFIXES"), :list_of_regex, []),
          disc_copies_node_name_prefixes: get_env!("MNESIAR_DISC_COPIES_NODE_NAME_PREFIXES"), :list_of_regex),
          disc_only_copies_node_name_prefixes: get_env!("MNESIAR_DISC_ONLY_COPIES_NODE_NAME_PREFIXES"), :list_of_regex, []),
          master_nodes: get_env!("MNESIAR_MASTER_NODES"), :list_of_atoms),
          # In seconds
          cached_data_ttl: get_env!("MNESIAR_CACHED_DATA_TTL"), :integer, 300)
        }
      ],
      # In milliseconds
      wait_for_tables_timeout: get_env!("MNESIAR_WAIT_FOR_TABLES_TIMEOUT"), :integer, 60_000),
      # In seconds
      wait_for_start_timeout: get_env!("MNESIAR_WAIT_FOR_START"), :integer, 10),
      # In seconds
      wait_for_stop_timeout: get_env!("MNESIAR_WAIT_FOR_STOP"), :integer, 10)
    }

  config :mnesia,
    dir: String.to_charlist(get_env!("MNESIAR_IN_MEMORY_DB_PATH"), :string, ".") <> "/Mnesia.#{Node.self()}"),
    dc_dump_limit: get_env!("MNESIAR_IN_MEMORY_DC_DUMP_LIMIT"), :integer, 40),
    dump_log_write_threshold: get_env!("MNESIAR_IN_MEMORY_DUMP_LOG_WRITE_THRESHOLD"), :integer, 50_000)

if config_env() in [:dev] do
end

if config_env() in [:prod] do
end
```


