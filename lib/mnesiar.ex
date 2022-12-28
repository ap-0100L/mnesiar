defmodule Mnesiar do
  ##############################################################################
  ##############################################################################
  @moduledoc """
  Documentation for Mnesiar.
  """

  use GenServer
  use Utils

  alias Mnesiar.Repo, as: Repo

  alias __MODULE__, as: SelfModule

  ##############################################################################
  @doc """
  Supervisor's child specification
  """
  def child_spec(opts) do
    %{
      id: SelfModule,
      start: {SelfModule, :start_link, [opts]}
    }
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def start_link(config: config, opts: opts) do
    GenServer.start_link(SelfModule, config, Keyword.put(opts, :name, SelfModule))
  end

  ##############################################################################
  @doc """
  ## Function
  """
  @impl true
  def init(%{wait_for_start_timeout: wait_for_start_timeout, creator_node: creator_node, entities: entities} = state) do
    state =
      UniError.rescue_error!(
        (
          mode = state[:mode]
          Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Mode #{inspect(mode)}")

          state =
            case mode do
              :standalone ->
                Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I am Mnesiar in standalone mode. Init started")

              :cluster ->
                Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I am Mnesiar in cluster mode. Init started")

                Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I will try to enable notification monitor on node connection events")

                result = :net_kernel.monitor_nodes(true)

                if :ok != result do
                  UniError.raise_error!(:CODE_CAN_NOT_ENABLE_MONITOR_ERROR, ["Can not enable notification monitor on node connection events"], reason: result)
                end

                state

              unexpected ->
                UniError.raise_error!(:CODE_UNKNOWN_SERVER_MODE_ERROR, ["Unknown server mode"], previous: unexpected)
            end

          :ok = Repo.start!(wait_for_start_timeout)
          :ok = Repo.init_table_states!(entities)

          if creator_node == Node.self() do
            Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I am creator node")

            {:ok, is_local_empty} = Repo.ensure_db_is_empty!()

            if is_local_empty do
              Logger.warn("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] In-memory DB is empty. I will try init it")
              :ok = Mnesiar.init_local!(state)
            else
              Logger.warn("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] In-memory DB is not empty. Skipping init")
            end
          else
            Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I am not creator, creator node is #{creator_node}")
          end

          state
        )
      )

    Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I completed init part")
    {:ok, state}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  @impl true
  def handle_call(:get_cookie, _from, state) do
    {:reply, {:ok, state[:cookie]}, state}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, {:ok, state}, state}
  end

  @impl true
  def handle_call(:start!, _from, %{wait_for_start_timeout: wait_for_start_timeout} = state) do
    :ok = Repo.start!(wait_for_start_timeout)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:set_state!, %{entities: entities} = new_state}, _from, state) do
    Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I will try set new Mnesiar state")
    {:ok, new_state} = get_config!(new_state)

    :ok = Repo.set_table_states!(entities)

    Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Mnesiar state seted successfully")
    {:reply, :ok, new_state}
  end

  #  ##############################################################################
  #  @doc """
  #
  #  """
  #  @impl true
  #  def handle_cast({:set_state!, new_state}, state) do
  #    {:ok, new_state} = get_config!(new_state)
  #    entities = new_state[:entities]
  #
  #    Utils.enum_each!(
  #      entities,
  #      fn et ->
  #        module = et[:module]
  #        {:ok, table_name} = apply(module, :get_table_name, [])
  #
  #        et = Map.put(et, :table_name, table_name)
  #
  #        :ok = StateUtils.set_state!(module, et)
  #      end
  #    )
  #
  #    {:noreply, new_state}
  #  end

  ##############################################################################
  @doc """
  ## Function
  """
  @impl true
  def handle_info({:nodeup, node}, %{entities: entities, mode: mode, leader_node: leader_node, cookie: local_cookie} = state) do
    state =
      UniError.rescue_error!(
        (
          Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Mode #{inspect(mode)}; action :nodeup")

          case mode do
            :standalone ->
              UniError.raise_error!(:CODE_WRONG_SERVER_MODE_ERROR, ["Wrong server mode"], mode: mode)

            :cluster ->
              Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Node #{inspect(node)} connected")

              is_leader = leader_node == Node.self()

              if is_leader do
                Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I am Mnesiar leader node, got node #{inspect(node)}")

                result =
                  UniError.rescue_error!(
                    (
                      {:ok, remote_cookie} = RPCUtils.call_rpc!(node, Mnesiar, :get_cookie, [])

                      if remote_cookie == local_cookie do
                        {:ok, true}
                      else
                        {:ok, false}
                      end
                    ),
                    false
                  )

                cluster_member =
                  case result do
                    {:ok, result} -> result
                    _ -> false
                  end

                if cluster_member do
                  Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Mnesiar node #{inspect(node)} authenticated successfully")

                  Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I will sync state of remote node #{inspect(node)}")
                  :ok = RPCUtils.call_rpc!(node, Mnesiar, :set_state!, [state])

                  Mnesiar.init_remote!(state, node)
                end

                if cluster_member do
                  {:ok, is_remote_empty} = RPCUtils.call_rpc!(node, Mnesiar.Repo, :ensure_db_is_empty!, [])

                  if not is_remote_empty do
                    {:ok, result} = RPCUtils.call_rpc!(node, Mnesiar.Repo, :set_master_nodes!, [entities])
                  end
                end
              end

              state

            unexpected ->
              UniError.raise_error!(:CODE_UNKNOWN_SERVER_MODE_ERROR, ["Unknown server mode"], previous: unexpected)
          end
        )
      )

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    state =
      UniError.rescue_error!(
        (
          mode = state[:mode]

          Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Mode #{inspect(mode)}; action :nodedown")

          # nodes = [Node.self | Node.list()]
          nodes = Node.list()

          case mode do
            :standalone ->
              UniError.raise_error!(:CODE_WRONG_SERVER_MODE_ERROR, ["Wrong server mode"], mode: mode)

            :cluster ->
              Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Node #{inspect(node)} disconnected")

              state

            unexpected ->
              UniError.raise_error!(:CODE_UNKNOWN_SERVER_MODE_ERROR, ["Unknown server mode"], previous: unexpected)
          end
        )
      )

    {:noreply, state}
  end

  def init_local!(
        %{
          entities: entities,
          schema_ram_copies_node_name_prefixes: schema_ram_copies_node_name_prefixes,
          schema_disc_copies_node_name_prefixes: schema_disc_copies_node_name_prefixes,
          wait_for_tables_timeout: wait_for_tables_timeout,
          wait_for_start_timeout: wait_for_start_timeout,
          wait_for_stop_timeout: wait_for_stop_timeout
        } = state
      ) do
    node = Node.self()

    :ok = Repo.stop!(wait_for_stop_timeout)
    :ok = Repo.create_schema!(wait_for_tables_timeout, [node])
    :ok = Repo.start!(wait_for_start_timeout)

    schema_ram_copies_nodes =
      if schema_ram_copies_node_name_prefixes == :all do
        [node]
      else
        {:ok, nodes} = Utils.get_nodes_list_by_prefixes!(schema_ram_copies_node_name_prefixes, [node])

        nodes
      end

    schema_disc_copies_nodes =
      if schema_disc_copies_node_name_prefixes == :all do
        [node]
      else
        {:ok, nodes} = Utils.get_nodes_list_by_prefixes!(schema_disc_copies_node_name_prefixes, [node])

        nodes
      end

    if schema_ram_copies_nodes != [] and schema_disc_copies_nodes != [] do
      UniError.raise_error!(
        :CODE_CONFLICT_SCHEMA_STORAGE_TYPE_ERROR,
        ["Node can not be disc_copies and ram_copies schema storage type in same time"],
        node: node,
        schema_ram_copies_node_name_prefixes: schema_ram_copies_node_name_prefixes,
        schema_disc_copies_node_name_prefixes: schema_disc_copies_node_name_prefixes
      )
    end

    if schema_ram_copies_nodes != [] or schema_disc_copies_nodes != [] do
      storage_type =
        case {schema_ram_copies_nodes, schema_disc_copies_nodes} do
          {[node], []} ->
            Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Node #{node} is RAM copies node")
            :ram_copies

          {[], [node]} ->
            Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Node #{node} is DISC copies node")
            :disc_copies
        end

      :ok = Repo.change_table_copy_storage_type!(:schema, node, storage_type)
    end

    {:ok, table_names} =
      Utils.enum_reduce_to_list!(entities, fn entity ->
        module = entity[:module]
        ram_copies_node_name_prefixes = entity[:ram_copies_node_name_prefixes]
        disc_copies_node_name_prefixes = entity[:disc_copies_node_name_prefixes]
        disc_only_copies_node_name_prefixes = entity[:disc_only_copies_node_name_prefixes]

        {:ok, ram_copies_nodes} = Utils.get_nodes_list_by_prefixes!(ram_copies_node_name_prefixes, [node])

        {:ok, disc_copies_nodes} = Utils.get_nodes_list_by_prefixes!(disc_copies_node_name_prefixes, [node])

        {:ok, disc_only_copies_nodes} = Utils.get_nodes_list_by_prefixes!(disc_only_copies_node_name_prefixes, [node])

        :ok =
          apply(module, :create_table!, [
            disc_copies_nodes,
            ram_copies_nodes,
            disc_only_copies_nodes
          ])

        {:ok, table_name} = apply(module, :get_table_name, [])

        master_nodes = entity[:master_nodes]

        {:ok, table_name}
      end)

    Repo.wait_for_tables!(table_names, wait_for_tables_timeout)
    {:ok, result} = Repo.set_master_nodes!(entities)

    Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Set master nodes result #{inspect(result)}")

    Utils.enum_each!(entities, fn e ->
      module = e[:module]

      is_load_exported = Kernel.function_exported?(module, :load, 0)

      if is_load_exported do
        {:ok, table_name} = module.get_table_name()

        Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I will load data to table #{table_name} (module #{module}) in in-memory DB")

        {:ok, {result, it_took_time}} = count_time(module.load())

        Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Data loaded to table #{table_name} module #{module}) in in-memory DB successfully, it took #{it_took_time / 1_000_000}ms")
      end

      :ok
    end)
  end

  def init_remote!(
        %{
          remote_access_only_node_name_prefixes: remote_access_only_node_name_prefixes,
          schema_ram_copies_node_name_prefixes: schema_ram_copies_node_name_prefixes,
          schema_disc_copies_node_name_prefixes: schema_disc_copies_node_name_prefixes,
          entities: entities,
          wait_for_tables_timeout: wait_for_tables_timeout,
          wait_for_start_timeout: wait_for_start_timeout
        } = state,
        node
      ) do
    ## ========================================================================================================

    remote_access_only_nodes =
      if remote_access_only_node_name_prefixes == :all do
        [node]
      else
        {:ok, nodes} = Utils.get_nodes_list_by_prefixes!(remote_access_only_node_name_prefixes, [node])

        nodes
      end

    schema_ram_copies_nodes =
      if schema_ram_copies_node_name_prefixes == :all do
        [node]
      else
        {:ok, nodes} = Utils.get_nodes_list_by_prefixes!(schema_ram_copies_node_name_prefixes, [node])

        nodes
      end

    schema_disc_copies_nodes =
      if schema_disc_copies_node_name_prefixes == :all do
        [node]
      else
        {:ok, nodes} = Utils.get_nodes_list_by_prefixes!(schema_disc_copies_node_name_prefixes, [node])

        nodes
      end

    if schema_ram_copies_nodes != [] and schema_disc_copies_nodes != [] do
      UniError.raise_error!(
        :CODE_CONFLICT_SCHEMA_STORAGE_TYPE_ERROR,
        ["Node can not be disc_copies and ram_copies schema storage type in same time"],
        node: node,
        schema_ram_copies_node_name_prefixes: schema_ram_copies_node_name_prefixes,
        schema_disc_copies_node_name_prefixes: schema_disc_copies_node_name_prefixes,
        remote_access_only_nodes: remote_access_only_nodes,
        schema_ram_copies_nodes: schema_ram_copies_nodes,
        schema_disc_copies_nodes: schema_disc_copies_nodes
      )
    end

    if schema_ram_copies_nodes != [] or schema_disc_copies_nodes != [] do
      storage_type =
        case {schema_ram_copies_nodes, schema_disc_copies_nodes} do
          {[node], []} ->
            Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Node #{node} is RAM copies node")
            :ram_copies

          {[], [node]} ->
            Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Node #{node} is DISC copies node")
            :disc_copies
        end

      :ok = Repo.ensure_tables_exists!(entities)

      :ok = RPCUtils.call_rpc!(node, Mnesiar, :start!, [])

      {:ok, db_nodes} = Repo.system_info!(:db_nodes)

      {:ok, nodes} = Repo.change_extra_db_nodes!([node])

      if node in db_nodes or node in remote_access_only_nodes do
        # Already in cluster
        Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Node #{node} already in cluster or remote access only node, skipping add table copy to node")
      else
        Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] I will try add node #{node} to cluster")

        {:ok, is_remote_empty} = RPCUtils.call_rpc!(node, Mnesiar.Repo, :ensure_db_is_empty!, [])

        if not is_remote_empty do
          UniError.raise_error!(:CODE_REMOTE_IN_MEMORY_DB_NOT_EMPTY_ERROR, ["In-memory DB on remote node #{node} is not empty"], node: node)
        end

        :ok = Repo.change_table_copy_storage_type!(:schema, node, storage_type)

        Utils.enum_each!(entities, fn entity ->
          module = entity[:module]
          {:ok, table_name} = apply(module, :get_table_name, [])

          ram_copies_node_name_prefixes = entity[:ram_copies_node_name_prefixes]
          disc_copies_node_name_prefixes = entity[:disc_copies_node_name_prefixes]
          disc_only_copies_node_name_prefixes = entity[:disc_only_copies_node_name_prefixes]

          ram_copies_nodes =
            if ram_copies_node_name_prefixes == :all do
              [node]
            else
              {:ok, nodes} = Utils.get_nodes_list_by_prefixes!(ram_copies_node_name_prefixes, [node])

              nodes
            end

          disc_copies_nodes =
            if disc_copies_node_name_prefixes == :all do
              [node]
            else
              {:ok, nodes} = Utils.get_nodes_list_by_prefixes!(disc_copies_node_name_prefixes, [node])

              nodes
            end

          disc_only_copies_nodes =
            if disc_only_copies_node_name_prefixes == :all do
              [node]
            else
              {:ok, nodes} = Utils.get_nodes_list_by_prefixes!(disc_only_copies_node_name_prefixes, [node])

              nodes
            end

          if (ram_copies_nodes != [] and (disc_copies_nodes != [] or disc_only_copies_nodes != [])) or
               (disc_copies_nodes != [] and (ram_copies_nodes != [] or disc_only_copies_nodes != [])) or
               (disc_only_copies_nodes != [] and (ram_copies_nodes != [] or disc_copies_nodes != [])) do
            UniError.raise_error!(:CODE_CONFLICT_TABLE_STORAGE_TYPE_ERROR, ["Table on node can not be ram_copies, disc_copies, disc_only_copies storage type in same time"], node: node, entity: entity)
          end

          if ram_copies_nodes != [] or disc_copies_nodes != [] or disc_only_copies_nodes != [] do
            storage_type =
              case {ram_copies_nodes, disc_copies_nodes, disc_only_copies_nodes} do
                {[node], [], []} ->
                  Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Table #{table_name} on node #{node} has RAM copy")
                  :ram_copies

                {[], [node], []} ->
                  Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Table #{table_name} on node #{node} has DISK copy")
                  :disc_copies

                {[], [], [node]} ->
                  Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Table  #{table_name} on node #{node} has DISK ONLY copy")
                  :disc_only_copies
              end

            :ok = Repo.add_table_copy_of_storage_type!(table_name, node, storage_type)

            :ok =
              RPCUtils.call_rpc!(node, Repo, :wait_for_tables!, [
                [table_name],
                wait_for_tables_timeout
              ])
          else
            Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Table #{table_name} does not present on node #{node} or node has remote access only")
          end
        end)
      end
    else
      Logger.info("[#{inspect(SelfModule)}][#{inspect(__ENV__.function)}] Node #{node} is not in-memory DB cluster node")
    end
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def start!() do
    GenServer.call(SelfModule, :start!)
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def set_state!(new_state) do
    # GenServer.cast(SelfModule, {:set_state!, new_state})
    GenServer.call(SelfModule, {:set_state!, new_state})
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def get_cookie() do
    GenServer.call(SelfModule, :get_cookie)
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def get_state() do
    GenServer.call(SelfModule, :get_state)
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def info!() do
    {:ok, state} = get_state()

    {:ok, mnesia_config} = Utils.get_app_all_env!(:mnesia)

    {:ok, system_info} = Mnesiar.Repo.system_info!(:all)
    {:ok, local_tables} = Mnesiar.Repo.system_info!(:local_tables)

    {:ok, tables_info} =
      Utils.enum_reduce_to_list!(local_tables, fn lt ->
        {:ok, table_info} = Mnesiar.Repo.table_info!(lt, :all)
        {:ok, %{table_name: lt, table_info: table_info}}
      end)

    {:ok,
     %{
       system_info: system_info,
       local_tables: local_tables,
       tables_info: tables_info,
       state: state,
       mnesia_config: mnesia_config
     }}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def get_config!(config \\ nil)

  def get_config!(config) do
    config =
      if is_nil(config) do
        {:ok, config} = Utils.get_app_env!(:mnesiar, :config)

        config
      else
        config
      end

    raise_if_empty!(config, :map, "Wrong config value")

    {:ok, mode} = raise_if_empty!(config, :mode, :atom, "Wrong mode value")

    {:ok, entities} = raise_if_empty!(config, :entities, :list, "Wrong entities value")

    {:ok, wait_for_tables_timeout} = raise_if_empty!(config, :wait_for_tables_timeout, :integer, "Wrong wait_for_tables_timeout value")

    leader_node = config[:leader_node]
    creator_node = config[:creator_node]

    if not is_nil(leader_node) do
      raise_if_empty!(leader_node, :atom, "Wrong leader_node value")
    end

    if not is_nil(creator_node) do
      raise_if_empty!(creator_node, :atom, "Wrong creator_node value")
    end

    Utils.enum_each!(entities, fn entity ->
      {:ok, module} = raise_if_empty!(entity, :module, :atom, "Wrong module value")

      ram_copies_node_name_prefixes = entity[:ram_copies_node_name_prefixes]
      disc_copies_node_name_prefixes = entity[:disc_copies_node_name_prefixes]
      disc_only_copies_node_name_prefixes = entity[:disc_only_copies_node_name_prefixes]

      ram_copies_is_empty = ram_copies_node_name_prefixes == []
      disc_copies_is_empty = disc_copies_node_name_prefixes == []
      disc_only_copies_is_empty = disc_only_copies_node_name_prefixes == []

      if ram_copies_node_name_prefixes == nil or
           disc_copies_node_name_prefixes == nil or
           disc_only_copies_node_name_prefixes == nil do
        UniError.raise_error!(:CODE_ENTITY_NODE_NAME_PREFIXES_CAN_NOT_BE_NIL_ERROR, ["ram_copies_node_name_prefixes, disc_copies_node_name_prefixes, disc_only_copies_node_name_prefixes can not be nil, use [] for empty list"], entity: entity)
      end

      if ram_copies_is_empty and
           disc_copies_is_empty and
           disc_only_copies_is_empty do
        UniError.raise_error!(:CODE_ENTITY_NODE_NAME_PREFIXES_CAN_NOT_BE_EMPTY_IN_SAME_TIME_ERROR, ["ram_copies_node_name_prefixes, disc_copies_node_name_prefixes, disc_only_copies_node_name_prefixes can not be empty in same time"], entity: entity)
      end

      if (ram_copies_node_name_prefixes == :all and
            (not disc_copies_is_empty or
               not disc_only_copies_is_empty)) or
           (disc_copies_node_name_prefixes == :all and
              (not ram_copies_is_empty or
                 not disc_only_copies_is_empty)) or
           (disc_only_copies_node_name_prefixes == :all and
              (not disc_copies_is_empty or
                 not ram_copies_is_empty)) do
        UniError.raise_error!(:CODE_CONFLICT_ENTITY_NODE_NAME_PREFIXES_ERROR, ["If one of ram_copies_node_name_prefixes, disc_copies_node_name_prefixes, disc_only_copies_node_name_prefixes is :all, other *_node_name_prefixes must be []"], entity: entity)
      end

      master_nodes = entity[:master_nodes]

      if master_nodes == nil do
        UniError.raise_error!(:CODE_TABLE_MASTER_NODES_CAN_NOT_BE_NIL_ERROR, ["master_nodes can not be nil, use [] for empty list"], entity: entity)
      end
    end)

    case mode do
      :standalone ->
        :ok

      :cluster ->
        {:ok, cookie} = raise_if_empty!(config, :cookie, :atom, "Wrong cookie value")

        remote_access_only_node_name_prefixes = config[:remote_access_only_node_name_prefixes]
        schema_ram_copies_node_name_prefixes = config[:schema_ram_copies_node_name_prefixes]
        schema_disc_copies_node_name_prefixes = config[:schema_disc_copies_node_name_prefixes]

        if remote_access_only_node_name_prefixes == nil or
             schema_ram_copies_node_name_prefixes == nil or
             schema_disc_copies_node_name_prefixes == nil do
          UniError.raise_error!(:CODE_SCHEMA_NODE_NAME_PREFIXES_CAN_NOT_BE_NIL_ERROR, ["remote_access_only_node_name_prefixes, schema_ram_copies_node_name_prefixes, schema_disc_copies_node_name_prefixes can not be nil, use [] for empty list"],
            schema_ram_copies_node_name_prefixes: schema_ram_copies_node_name_prefixes,
            schema_disc_copies_node_name_prefixes: schema_disc_copies_node_name_prefixes
          )
        end

        remote_access_only_is_empty = remote_access_only_node_name_prefixes == []
        schema_ram_copies_is_empty = schema_ram_copies_node_name_prefixes == []
        schema_disc_copies_is_empty = schema_disc_copies_node_name_prefixes == []

        if remote_access_only_is_empty and schema_ram_copies_is_empty and schema_disc_copies_is_empty do
          UniError.raise_error!(:CODE_SCHEMA_NODE_NAME_PREFIXES_CAN_NOT_BE_EMPTY_IN_SAME_TIME_ERROR, ["remote_access_only_node_name_prefixes, schema_ram_copies_node_name_prefixes, schema_disc_copies_node_name_prefixes can not be empty in same time"],
            remote_access_only_node_name_prefixes: remote_access_only_node_name_prefixes,
            schema_ram_copies_node_name_prefixes: schema_ram_copies_node_name_prefixes,
            schema_disc_copies_node_name_prefixes: schema_disc_copies_node_name_prefixes
          )
        end

        if (remote_access_only_node_name_prefixes == :all and (not schema_disc_copies_is_empty or not schema_disc_copies_is_empty)) or
             (schema_ram_copies_node_name_prefixes == :all and (not remote_access_only_is_empty or not schema_disc_copies_is_empty)) or
             (schema_disc_copies_node_name_prefixes == :all and (not remote_access_only_is_empty or not schema_ram_copies_is_empty)) do
          UniError.raise_error!(:CODE_CONFLICT_SCHEMA_NODE_NAME_PREFIXES_ERROR, ["If one of remote_access_only_node_name_prefixes, schema_ram_copies_node_name_prefixes, schema_disc_copies_node_name_prefixes is :all, other schema_*_node_name_prefixes must be []"],
            remote_access_only_node_name_prefixes: remote_access_only_node_name_prefixes,
            schema_ram_copies_node_name_prefixes: schema_ram_copies_node_name_prefixes,
            schema_disc_copies_node_name_prefixes: schema_disc_copies_node_name_prefixes
          )
        end

        :ok

      unexpected ->
        UniError.raise_error!(:CODE_UNKNOWN_IN_MEMORY_DB_SERVER_MODE_ERROR, ["Unknown in-memory DB server mode"],
          node: node,
          previous: unexpected
        )
    end

    {:ok, config}
  end

  ##############################################################################
  ##############################################################################
end
