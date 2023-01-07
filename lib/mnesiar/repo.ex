defmodule Mnesiar.Repo do
  ##############################################################################
  ##############################################################################
  @moduledoc """
  ## Module
  """

  use Utils

  alias :mnesia, as: Mnesia

  @schema_storage_types [:disc_copies, :ram_copies]
  @entity_storage_types [:disc_copies, :ram_copies, :disc_only_copies]
  @entity_types [:set, :ordered_set, :bag]
  @change_config_opts [:extra_db_nodes, :dc_dump_limit]

  ##############################################################################
  @doc """
  ## Function
  """
  def state!() do
    result = Mnesia.system_info(:is_running)

    case result do
      :yes ->
        {:ok, :CODE_IN_MEMMORY_DB_STARTED}

      :no ->
        {:ok, :CODE_IN_MEMMORY_DB_STOPPED}

      :starting ->
        {:ok, :CODE_IN_MEMMORY_DB_STARTING}

      :stopping ->
        {:ok, :CODE_IN_MEMMORY_DB_STOPPING}

      unexpected ->
        UniError.raise_error!(:CODE_STATE_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
    end
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def wait_for_start!({:ok, :CODE_IN_MEMMORY_DB_STARTED}, _count) do
    :ok
  end

  def wait_for_start!({:ok, :CODE_IN_MEMMORY_DB_STARTING}, count) do
    :timer.sleep(1000)
    wait_for_start!(state!(), count - 1)
  end

  def wait_for_start!({:ok, status}, _count) do
    UniError.raise_error!(:CODE_START_UNEXPECTED_IN_MEMORY_DB_STATE_ERROR, ["Unexpected in-memory DB state"], state: status)
  end

  def wait_for_start!(_status, 0) do
    UniError.raise_error!(:CODE_START_TIMEOUT_IN_MEMORY_DB_ERROR, ["In-memory DB start timeout"])
  end

  def wait_for_start!(wait_for_start_timeout) do
    wait_for_start!(state!(), wait_for_start_timeout)
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def start!(wait_for_start_timeout)
      when is_nil(wait_for_start_timeout) or not is_number(wait_for_start_timeout),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["wait_for_start_timeout cannot be nil; wait_for_start_timeout must be a number"])

  def start!(wait_for_start_timeout) do
    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will try start in-memory DB and wait #{inspect(wait_for_start_timeout)}s")

    {result, it_took_time} =
      case Mnesia.start() do
        :ok ->
          {:ok, {result, it_took_time}} = count_time(wait_for_start!(wait_for_start_timeout))

          {result, it_took_time}

        {:error, reason} ->
          UniError.raise_error!(:CODE_CAN_NOT_START_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_CAN_NOT_START_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_CAN_NOT_START_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] In-memory DB successfully started, it took time #{it_took_time / 1_000_000}ms")

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def wait_for_stop!({:ok, :CODE_IN_MEMMORY_DB_STOPPED}, _count) do
    :ok
  end

  def wait_for_stop!({:ok, :CODE_IN_MEMMORY_DB_STOPPING}, count) do
    :timer.sleep(100)
    wait_for_stop!(state!(), count - 1)
  end

  def wait_for_stop!({:ok, status}, _count) do
    UniError.raise_error!(:CODE_STOP_UNEXPECTED_IN_MEMORY_DB_STATE_ERROR, ["Unexpected in-memory DB state"], state: status)
  end

  def wait_for_stop!(_status, 0) do
    UniError.raise_error!(:CODE_STOP_TIMEOUT_IN_MEMORY_DB_ERROR, ["In-memory DB start timeout"])
  end

  def wait_for_stop!(wait_for_stop_timeout) do
    wait_for_stop!(state!(), wait_for_stop_timeout)
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def stop!(wait_for_stop_timeout) do
    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will try stop in-memory DB and wait #{inspect(wait_for_stop_timeout)}s")

    {result, it_took_time} =
      case Mnesia.stop() do
        :stopped ->
          {:ok, {result, it_took_time}} = count_time(wait_for_stop!(wait_for_stop_timeout))

          {result, it_took_time}

        {:error, reason} ->
          UniError.raise_error!(:CODE_CAN_NOT_STOP_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_CAN_NOT_STOP_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_CAN_NOT_STOP_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] In-memory DB successfully stopped, it took time #{it_took_time / 1_000_000}ms")

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def change_extra_db_nodes!(nodes) do
    {:ok, extra_nodes} = change_config!(:extra_db_nodes, nodes)

    result =
      case extra_nodes do
        [] ->
          UniError.raise_error!(:CODE_CHANGE_CONFIG_EXTRA_NODES_NOT_CONNECTED_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], nodes: nodes)

        _ ->
          {:ok, extra_nodes}
      end

    Utils.enum_each!(
      nodes,
      fn n, extra_nodes ->
        if n not in extra_nodes do
          UniError.raise_error!(:CODE_CHANGE_CONFIG_EXTRA_NODES_NOT_CONNECTED_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], node: n, extra_nodes: extra_nodes)
        end
      end,
      extra_nodes
    )

    result
  end

  def change_config!(opt, value)
      when is_nil(opt) or is_nil(value) or
             not is_atom(opt) or (opt == :dc_dump_limit and not is_number(value)) or
             (opt == :extra_db_nodes and not is_list(value)) or opt not in @change_config_opts,
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["opt, value cannot be nil; opt must be an atom; value must be a list or number; opt must be one of #{inspect(@change_config_opts)}"], opt: opt, opts: @change_config_opts)

  def change_config!(opt, value) do
    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will try change config #{inspect(opt)} of in-memory DB on value #{inspect(value)}")

    result = Mnesia.change_config(opt, value)

    result =
      case result do
        {:ok, result} ->
          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] In-memory DB successfully configured #{inspect(opt)} with result: #{inspect(result)}")

          {:ok, result}

        {:error, reason} ->
          UniError.raise_error!(:CODE_CHANGE_CONFIG_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_CHANGE_CONFIG_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_CHANGE_CONFIG_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def create_schema!(nodes) do
    Logger.info("[#{Node.self()}] I will try create schema in in-memory DB on nodes #{inspect(nodes)}")

    result = Mnesia.create_schema(nodes)

    case result do
      :ok ->
        :ok

      {:error, {_node, {:already_exists, node}}} ->
        UniError.raise_error!(:CODE_SCHEMA_ALREADY_EXISTS_ERROR, ["Error occurred while process operation in-memory DB"], node: node)

      {:error, reason} ->
        UniError.raise_error!(:CODE_CREATE_SCHEMA_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

      {:aborted, reason} ->
        UniError.raise_error!(:CODE_CREATE_SCHEMA_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

      unexpected ->
        UniError.raise_error!(:CODE_CREATE_SCHEMA_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
    end

    Logger.info("[#{Node.self()}] In-memory DB schema created on nodes #{inspect(nodes)} successfully")

    result
  end

  ##############################################################################
  @doc """
  Delete
  """
  def table_info!(table, key) do
    result = Mnesia.table_info(table, key)

    result =
      case result do
        {:error, reason} ->
          UniError.raise_error!(:CODE_TABLE_INFO_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, key: key)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_TABLE_INFO_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, key: key)

        value ->
          value
      end

    {:ok, result}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def create_table!(table, opts, indexes \\ [])

  def create_table!(table, opts, indexes)
      when is_nil(table) or is_nil(opts) or is_nil(indexes) or not is_atom(table) or
             not is_list(opts) or not is_list(indexes),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["table, opts, indexes cannot be nil; table must be an atom; opts, indexes must be a list"])

  def create_table!(table, opts, indexes) do
    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will try create table #{table} with indexes #{inspect(indexes)}.")

    result =
      Mnesia.create_table(
        table,
        opts
      )

    result =
      case result do
        value when value in [:ok, {:atomic, :ok}] ->
          for item <- indexes do
            raise_if_empty!(item, :atom, "Wrong index value")

            result = Mnesia.add_table_index(table, item)

            case result do
              value when value in [:ok, {:atomic, :ok}] ->
                :ok

              {:error, reason} ->
                UniError.raise_error!(:CODE_ADD_TABLE_INDEX_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, table_name: table, index_field: item)

              {:aborted, reason} ->
                UniError.raise_error!(:CODE_ADD_TABLE_INDEX_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, table_name: table, index_field: item)

              unexpected ->
                UniError.raise_error!(:CODE_ADD_TABLE_INDEX_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected, table_name: table, index_field: item)
            end
          end

          :ok

        {:aborted, {:already_exists, _table_name}} ->
          UniError.raise_error!(:CODE_TABLE_ALREADY_EXISTS_ERROR, ["Error occurred while process operation in-memory DB"], table_name: table)

        {:error, reason} ->
          UniError.raise_error!(:CODE_CREATE_TABLE_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, table_name: table)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_CREATE_TABLE_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, table_name: table)

        unexpected ->
          UniError.raise_error!(:CODE_CREATE_TABLE_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected, table_name: table)
      end

    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Table #{table} with indexes #{inspect(indexes)} created successfully.")

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def ensure_tables_exists!(entities)
      when is_nil(entities) or not is_list(entities),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["entities cannot be nil; entities must be a list"])

  def ensure_tables_exists!(entities) do
    {:ok, local_tables} = system_info!(:local_tables)

    Utils.enum_each!(
      entities,
      fn et, local_tables ->
        module = et[:module]
        {:ok, table_name} = apply(module, :get_table_name, [])

        if table_name not in local_tables do
          UniError.raise_error!(:CODE_TABLE_NOT_EXISTS_IN_LOCAL_TABLES_IN_MEMORY_DB_ERROR, ["Table not exists in local tables in-memory DB"], table_name: table_name, local_tables: local_tables)
        end
      end,
      local_tables
    )

    :ok
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def init_table_states!(entities)
      when is_nil(entities) or not is_list(entities),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["entities cannot be nil; entities must be a list"])

  def init_table_states!(entities) do
    Utils.enum_each!(
      entities,
      fn et ->
        module = et[:module]
        {:ok, table_name} = apply(module, :get_table_name, [])

        et = Map.put(et, :table_name, table_name)

        {:ok, _pid} = StateUtils.init_state!(module, et)
      end
    )

    :ok
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def set_table_states!(entities)
      when is_nil(entities) or not is_list(entities),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["entities cannot be nil; entities must be a list"])

  def set_table_states!(entities) do
    Utils.enum_each!(
      entities,
      fn et ->
        module = et[:module]
        {:ok, table_name} = apply(module, :get_table_name, [])

        et = Map.put(et, :table_name, table_name)

        :ok = StateUtils.set_state!(module, et)
      end
    )

    :ok
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def ensure_db_is_empty!() do
    {:ok, local_tables} = system_info!(:local_tables)

    {:ok, local_tables == [:schema]}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def set_master_nodes!(table, nodes)
      when is_nil(table) or is_nil(nodes) or not is_list(nodes) or not is_atom(table),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["table, nodes cannot be nil; nodes must be a list; table must be an atom"])

  def set_master_nodes!(table, nodes) do
    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will try set master nodes #{inspect(nodes)} for table #{table}.")

    result =
      Mnesia.set_master_nodes(
        table,
        nodes
      )

    result =
      case result do
        value when value in [:ok, {:atomic, :ok}] ->
          :ok

        {:error, reason} ->
          UniError.raise_error!(:CODE_SET_MASTER_NODES_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, table_name: table, nodes: nodes)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_SET_MASTER_NODES_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, table_name: table, nodes: nodes)

        unexpected ->
          UniError.raise_error!(:CODE_SET_MASTER_NODES_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected, table_name: table, nodes: nodes)
      end

    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Master nodes #{inspect(nodes)} for table #{table} were set successfully.")

    result
  end

  def set_master_nodes!(entities)
      when is_nil(entities) or not is_list(entities),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["entities cannot be nil; entities must be a list"])

  def set_master_nodes!(entities) do
    result =
      Utils.enum_reduce_to_list!(
        entities,
        fn et ->
          module = et[:module]
          master_nodes = et[:master_nodes]
          {:ok, table_name} = apply(module, :get_table_name, [])

          result =
            UniError.rescue_error!(
              set_master_nodes!(table_name, master_nodes),
              false
            )

          result =
            case result do
              :ok -> :ok
              _ -> result
            end

          {:ok, %{table_name: table_name, result: result}}
        end
      )

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def system_info!(option)
      when is_nil(option) or not is_atom(option),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["option cannot be nil; option must be an atom"])

  def system_info!(key) do
    result = Mnesia.system_info(key)

    result =
      case result do
        {:error, reason} ->
          UniError.raise_error!(:CODE_SYSTEM_INFO_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, key: key)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_SYSTEM_INFO_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, key: key)

        value ->
          value
      end

    {:ok, result}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def wait_for_tables!(tables, wait_for_tables_timeout)
      when is_nil(tables) or is_nil(wait_for_tables_timeout) or not is_list(tables) or
             not is_integer(wait_for_tables_timeout) or tables == [],
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["table, wait_for_tables_timeout cannot be nil; table must be a list; wait_for_tables_timeout must be an integer; table can not be []"])

  def wait_for_tables!(tables, wait_for_tables_timeout) do
    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will wait the tables #{inspect(tables)}, #{wait_for_tables_timeout}ms")

    {:ok, {result, it_took_time}} = count_time(Mnesia.wait_for_tables(tables, wait_for_tables_timeout))

    result =
      case result do
        value when value in [:ok, {:atomic, :ok}] ->
          :ok

        {:error, reason} ->
          UniError.raise_error!(:CODE_WAIT_FOR_TABLES_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_WAIT_FOR_TABLES_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_WAIT_FOR_TABLES_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I waited for the tables #{inspect(tables)}, it took #{it_took_time / 1_000_000}ms")

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def add_table_copy_of_storage_type!(table, node, storage_type)
      when is_nil(table) or is_nil(node) or is_nil(storage_type) or not is_atom(table) or
             not is_atom(node) or not is_atom(storage_type) or
             storage_type not in @entity_storage_types,
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["table, node, storage_type cannot be nil; table, node, storage_type must be an atom; storage_type must be on of #{inspect(@entity_storage_types)}"])

  def add_table_copy_of_storage_type!(table, node, storage_type) do
    result = Mnesia.add_table_copy(table, node, storage_type)

    case result do
      value when value in [:ok, {:atomic, :ok}] ->
        Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Table #{inspect(table)} with storage_type #{inspect(storage_type)} on node #{inspect(node)} added successfully")

        :ok

      {:aborted, {:already_exists, table_name, ret_node}} ->
        UniError.raise_error!(:CODE_ADD_TABLE_COPY_IN_MEMORY_DB_ALREADY_EXISTS_ERROR, ["Error occurred while process operation in-memory DB"], table_name: table_name, node: ret_node, storage_type: storage_type)

      {:error, reason} ->
        UniError.raise_error!(:CODE_ADD_TABLE_COPY_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, table_name: table, storage_type: storage_type)

      {:aborted, reason} ->
        UniError.raise_error!(:CODE_ADD_TABLE_COPY_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, table_name: table, storage_type: storage_type)

      unexpected ->
        UniError.raise_error!(:CODE_ADD_TABLE_COPY_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected, table_name: table, storage_type: storage_type)
    end
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def change_table_copy_storage_type!(table, node, storage_type)
      when is_nil(table) or is_nil(node) or is_nil(storage_type) or not is_atom(table) or
             not is_atom(node) or not is_atom(storage_type) or
             (table != :schema and storage_type not in @entity_storage_types) or
             (table == :schema and storage_type not in @schema_storage_types),
      do:
        UniError.raise_error!(
          :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
          ["table, node, storage_type cannot be nil; table, node, storage_type must be an atom; storage_type must be on of #{inspect(@entity_storage_types ++ @schema_storage_types)}"],
          entity_storage_types: @entity_storage_types,
          schema_storage_types: @schema_storage_types
        )

  def change_table_copy_storage_type!(table, node, storage_type) do
    Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will try change table copy #{inspect(table)} on storage_type #{inspect(storage_type)} on node #{inspect(node)}")

    {:ok, schema_storage_type} =
      if node == Node.self() do
        table_info!(table, :storage_type)
      else
        RPCUtils.call_rpc!(node, Mnesiar.Repo, :table_info!, [table, :storage_type])
      end

    result =
      if schema_storage_type != storage_type do
        Mnesia.change_table_copy_type(table, node, storage_type)
      else
        {:ok, :CODE_TABLE_ALREADY_CURRENT_TYPE}
      end

    result =
      case result do
        value when value in [:ok, {:atomic, :ok}] ->
          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Table #{inspect(table)} storage_type #{inspect(storage_type)} successfully changed on node #{inspect(node)}, previous storage_type #{inspect(schema_storage_type)}")

          :ok

        {:ok, :CODE_TABLE_ALREADY_CURRENT_TYPE} ->
          Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Table #{inspect(table)} already storage_type #{inspect(storage_type)} on node #{inspect(node)}")
          :ok

        {:aborted, {:already_exists, _table_name, _ret_node, _ret_type}} ->
          UniError.raise_error!(:CODE_CHANGE_TABLE_COPY_STORAGE_TYPE_IN_MEMORY_DB_ALREADY_EXISTS_ERROR, ["Error occurred while process operation in-memory DB"], table_name: table, storage_type: storage_type)

        {:error, reason} ->
          UniError.raise_error!(:CODE_CHANGE_TABLE_COPY_STORAGE_TYPE_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, table_name: table, storage_type: storage_type)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_CHANGE_TABLE_COPY_STORAGE_TYPE_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, table_name: table, storage_type: storage_type)

        unexpected ->
          UniError.raise_error!(:CODE_CHANGE_TABLE_COPY_STORAGE_TYPE_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected, table_name: table, storage_type: storage_type)
      end

    result
  end

  ##############################################################################
  @doc """
  Save
  """
  def save!(table, o) do
    result = Mnesia.transaction(fn -> Mnesia.write(table, o, :write) end)

    result =
      case result do
        value when value in [:ok, {:atomic, :ok}] ->
          {:ok, o}

        {:error, reason} ->
          UniError.raise_error!(:CODE_SAVE_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_SAVE_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_SAVE_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    result
  end

  ##############################################################################
  @doc """
  read
  """
  def get_by_id!(table, id) do
    result = Mnesia.transaction(fn -> Mnesia.read(table, id) end)

    result =
      case result do
        {:atomic, []} ->
          {:ok, :CODE_NOTHING_FOUND}

        {:atomic, [obj | _]} ->
          {:ok, obj}

        {:error, reason} ->
          UniError.raise_error!(:CODE_GET_BY_ID_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_GET_BY_ID_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_GET_BY_ID_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def read_all!(table) do
    result =
      Mnesia.transaction(fn ->
        Mnesia.foldl(
          fn item, accum ->
            [item | accum]
          end,
          [],
          table
        )
      end)

    result =
      case result do
        {:atomic, []} ->
          {:ok, :CODE_NOTHING_FOUND}

        {:atomic, [obj | _]} ->
          {:ok, obj}

        {:error, reason} ->
          UniError.raise_error!(:CODE_READ_ALL_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_READ_ALL_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_READ_ALL_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    result
  end

  ##############################################################################
  @doc """
  Delete
  """
  def delete!(table, id) do
    result = Mnesia.transaction(fn -> Mnesia.delete({table, id}) end)

    result =
      case result do
        value when value in [:ok, {:atomic, :ok}] ->
          :ok

        {:error, reason} ->
          UniError.raise_error!(:CODE_DELETE_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_DELETE_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_DELETE_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    result
  end

  ##############################################################################
  @doc """
  Delete
  """
  def delete_object!(table, object) do
    result = Mnesia.transaction(fn -> Mnesia.delete_object(table, object, :sticky_write) end)

    result =
      case result do
        value when value in [:ok, {:atomic, :ok}] ->
          :ok

        {:error, reason} ->
          UniError.raise_error!(:CODE_DELETE_OBJECT_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_DELETE_OBJECT_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_DELETE_OBJECT_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    result
  end

  ##############################################################################
  @doc """
  Delete
  """
  def delete_object_list!(table, list)
      when is_nil(table) or is_nil(list) or not is_list(list) or not is_atom(table),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["table, list cannot be nil; table, list must be a list"])

  def delete_object_list!(table, list) do
    count =
      if length(list) > 0 do
        Enum.reduce(
          list,
          0,
          fn item, accum ->
            delete_object!(table, item)
            accum + 1
          end
        )
      else
        0
      end

    {:ok, count}
  end

  ##############################################################################
  @doc """
  Match object
  """
  def find_with_match_object!(table, pattern \\ {:_})

  def find_with_match_object!(table, pattern)
      when is_nil(table) or is_nil(pattern) or not is_tuple(pattern) or not is_atom(table),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["table, pattern cannot be nil; table must be an atom; pattern must be a tuple"])

  def find_with_match_object!(table, pattern) do
    result = Mnesia.transaction(fn -> Mnesia.match_object(table, pattern, :read) end)

    result =
      case result do
        {:atomic, []} ->
          {:ok, :CODE_NOTHING_FOUND}

        {:atomic, result} ->
          {:ok, result}

        {:error, reason} ->
          UniError.raise_error!(:CODE_FIND_WITH_MATCH_OBJECT_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_FIND_WITH_MATCH_OBJECT_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_FIND_WITH_MATCH_OBJECT_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def find_with_index!(table, index, value) do
    result = Mnesia.transaction(fn -> Mnesia.index_read(table, value, index) end)

    result =
      case result do
        {:atomic, []} ->
          {:ok, :CODE_NOTHING_FOUND}

        {:atomic, result} ->
          {:ok, result}

        {:error, reason} ->
          UniError.raise_error!(:CODE_FIND_WITH_INDEX_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_FIND_WITH_INDEX_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason)

        unexpected ->
          UniError.raise_error!(:CODE_FIND_WITH_INDEX_IN_MEMORY_DB_UNEXPECTED_ERROR, ["Unexpected error occurred while process operation in-memory DB"], previous: unexpected)
      end

    result
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def get_table_opts(
        record_name,
        table_type,
        disc_copies_nodes \\ nil,
        ram_copies_nodes \\ nil,
        disc_only_copies_nodes \\ nil,
        attributes \\ nil
      ) do
    opts = [
      {:record_name, record_name},
      {:type, table_type}
    ]

    attribs =
      if :ok === Utils.is_not_empty(attributes, :list) do
        [{:attributes, attributes}]
      else
        []
      end

    disc_copies =
      if :ok === Utils.is_not_empty(disc_copies_nodes, :list) do
        [{:disc_copies, disc_copies_nodes}]
      else
        []
      end

    ram_copies =
      if :ok === Utils.is_not_empty(ram_copies_nodes, :list) do
        [{:ram_copies, ram_copies_nodes}]
      else
        []
      end

    disc_only_copies =
      if :ok === Utils.is_not_empty(disc_only_copies_nodes, :list) do
        [{:disc_only_copies, disc_only_copies_nodes}]
      else
        []
      end

    result = opts ++ attribs ++ disc_copies ++ ram_copies ++ disc_only_copies

    {:ok, result}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  defp find_index([k | _], k, i), do: i
  defp find_index([_ | t], k, i), do: find_index(t, k, i + 1)
  defp find_index([], _k, _i), do: nil

  def get!(attributes, var, key) do
    index =
      find_index(attributes, key, 1) ||
        UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["record #{inspect(:erlang.element(1, var))} does not have the key: #{inspect(key)}"], attributes: attributes, key: key)

    result = elem(var, index)
    {:ok, result}
  end

  def put!(attributes, var, key, value) do
    index =
      find_index(attributes, key, 1) ||
        UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["record #{inspect(:erlang.element(1, var))} does not have the key: #{inspect(key)}"], attributes: attributes, key: key)

    result = put_elem(var, index, value)
    {:ok, result}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def record_to_map(attributes, record) do
    result =
      Enum.reduce(
        attributes,
        %{},
        fn attribute, accum ->
          {:ok, value} = get!(attributes, record, attribute)
          Map.put(accum, attribute, value)
        end
      )

    {:ok, result}
  end

  ##############################################################################
  @doc """
  ## Function
  """
  def map_to_record(attributes, record_name, map) do
    result =
      Enum.reduce(
        attributes,
        {record_name},
        fn attribute, accum ->
          {:ok, k_atom} = Utils.string_to_atom(attribute)

          value_from_atom_key =
            case Map.fetch(map, k_atom) do
              {:ok, value} -> value
              :error -> nil
            end

          {:ok, k_string} = Utils.atom_to_string(attribute)

          value_from_string_key =
            case Map.fetch(map, k_string) do
              {:ok, value} -> value
              :error -> nil
            end

          value =
            if not is_nil(value_from_atom_key) and not is_nil(value_from_string_key) do
              UniError.raise_error!(:CODE_MAP_HAS_ATOM_AND_STRING_KEYS_WITH_SAME_NAME_ERROR, ["Map has atom and string keys with same name"], record_name: record_name, key: attribute)
            else
              value_from_atom_key || value_from_string_key
            end

          Tuple.append(accum, value)
        end
      )

    {:ok, result}
  end

  ##############################################################################
  @doc """
  ### Function

  ```
    @impl true
    def load() do
      {:ok, table_name} = get_table_name()
      Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will load data to table {table_name} in in-memory DB")
      #
      Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Data loaded to table {table_name} in in-memory DB successfully")
      :ok
    end
  ```
  """
  @callback load() :: term

  ###########################################################################
  @doc """
  ### Function

  ```
      @impl true
      def get_persistent!(filters, limit, opts \\ [])

      def get_persistent!(filters, limit, opts)
          when (not is_nil(limit) and (not is_integer(limit) or limit <= 0)) or (not is_nil(filters) and not is_map(filters) and not is_list(filters)) or not is_list(opts),
          do:
            UniError.raise_error!(
              :CODE_WRONG_FUNCTION_ARGUMENT_ERROR,
              ["opts cannot be nil; filters if not nil must be a map or a list; limit if not nil must be an integer and must be > 0; opts must be a list"]
            )

      @impl true
      def get_persistent!(filters, limit, opts) do
        if is_nil(@persistent_schema) do
          UniError.raise_error!(:CODE_PERSISTENT_SCHEMA_IS_NIL_MNESIAR_ERROR, ["Persistent schema is nil"], module: SelfModule)
        end

        query =
          from(
            o in @persistent_schema,
            select: o
          )

        query =
          if is_nil(limit) do
            query
          else
            limit(query, ^limit)
          end

        {:ok, query} = @persistent_schema.simple_where_filter!(query, filters)
        preloads = Keyword.get(opts, :preloads, nil)
        opts = Keyword.delete(opts, :preloads)

        result = @persistent_schema.get_by_query!(query, opts)

        if result == {:ok, :CODE_NOTHING_FOUND} do
          result
        else
          {:ok, items} = result

          items =
            Enum.reduce(
              items,
              [],
              fn item, accum ->
                item =
                  if is_nil(preloads) do
                    item
                  else
                    {:ok, item} = @persistent_schema.preload!(item, preloads)
                    item
                  end

                item = Map.from_struct(item)

                {:ok, item} = SelfModule.map_to_record(item)
                {:ok, item} = SelfModule.prepare!(item)
                {:ok, item} = SelfModule.save!(item)

                accum ++ [item]
              end
            )

          {:ok, items}
        end
      end
  ```
  OR
  ```
      @impl true
      def get_persistent!(_filters, _limit, _opts) do
        UniError.raise_error!(:CODE_PLEASE_IMPLEMENT_FUNCTION_ERROR, ["Please, implement function"], function: {:get_persistent!, 2}, module: SelfModule)
      end
  ```
  """
  @callback get_persistent!(filters :: any, limit :: any, opts :: any) :: term

  ###########################################################################
  @doc """
  ### Function

  ```
      @impl true
      def save_persistent!(o, async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      @impl true
      def save_persistent!(o, async, rescue_func, rescue_func_args, module) do
        if is_nil(@persistent_schema) do
          UniError.raise_error!(:CODE_PERSISTENT_SCHEMA_IS_NIL_MNESIAR_ERROR, ["Persistent schema is nil"], module: SelfModule)
        end

        {:ok, map} = SelfModule.record_to_map(o)

        # TODO: Here some manipulations with data. And save in persistent. Maybe in several linked transactions
        # @persistent_schema

        @persistent_schema.update!(map, async, rescue_func, rescue_func_args, module)

        {:ok, o}
      end
  ```

    OR

  ```
      @impl true
      def save_persistent!(o, async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      @impl true
      def save_persistent!(_o, _async, _rescue_func, _rescue_func_args, _module) do
        UniError.raise_error!(:CODE_PLEASE_IMPLEMENT_FUNCTION_ERROR, ["Please, implement function"], function: {:save_persistent!, 2}, module: SelfModule)
      end
  ```
  """
  @callback save_persistent!(o :: any, async :: any, rescue_func :: any, rescue_func_args :: any, module :: any) :: term

  @optional_callbacks load: 0

  ##############################################################################
  @doc """
  ## Function
  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use Utils

      import Record, only: [defrecord: 3]
      import Ecto.Query, only: [from: 2, limit: 2]

      alias Utils, as: Utils
      alias Mnesiar.Repo, as: MnesiarRepo
      alias __MODULE__, as: SelfModule

      @behaviour MnesiarRepo

      @table_name Keyword.fetch!(opts, :table_name)
      @record_name Keyword.fetch!(opts, :record_name)
      @attributes Keyword.fetch!(opts, :attributes) ++ [:timestamp]
      @indexes Keyword.fetch!(opts, :indexes)
      @table_type Keyword.fetch!(opts, :table_type)
      @persistent_schema Keyword.get(opts, :persistent_schema, nil)

      @logical_operators [:and, :or]

      ##############################################################################
      @doc """
      ### Function
      """
      defrecord(
        @record_name,
        @table_name,
        Enum.reduce(@attributes, [], fn attribute, accum ->
          :lists.append(accum, [{attribute, nil}])
        end)
      )

      ##############################################################################
      @doc """
      ### Function
      """
      def get_table_name() do
        {:ok, @table_name}
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def get_table_opts(
            disc_copies_nodes \\ nil,
            ram_copies_nodes \\ nil,
            disc_only_copies_nodes \\ nil
          )

      def get_table_opts(disc_copies_nodes, ram_copies_nodes, disc_only_copies_nodes) do
        MnesiarRepo.get_table_opts(
          @record_name,
          @table_type,
          disc_copies_nodes,
          ram_copies_nodes,
          disc_only_copies_nodes,
          @attributes
        )
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def get!(record, key) do
        MnesiarRepo.get!(@attributes, record, key)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def put!(record, key, value) do
        MnesiarRepo.put!(@attributes, record, key, value)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def record_to_map(record) do
        MnesiarRepo.record_to_map(@attributes, record)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def map_to_record(map) do
        MnesiarRepo.map_to_record(@attributes, @record_name, map)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def create_table!(
            disc_copies_nodes \\ nil,
            ram_copies_nodes \\ nil,
            disc_only_copies_nodes \\ nil
          )

      def create_table!(
            disc_copies_nodes,
            ram_copies_nodes,
            disc_only_copies_nodes
          ) do
        {:ok, opts} = get_table_opts(disc_copies_nodes, ram_copies_nodes, disc_only_copies_nodes)

        MnesiarRepo.create_table!(@table_name, opts, @indexes)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def add_table_copy_of_storage_type!(node, type) do
        MnesiarRepo.add_table_copy_of_storage_type!(@table_name, node, type)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def change_tables_copies_type!(node, type) do
        MnesiarRepo.change_table_copy_storage_type!(@table_name, node, type)
      end

      ##############################################################################
      @doc """
      ### Function
      ```
          opts = [kep_timestamp: true]
      ```
      """
      def save!(o, opts \\ [])

      def save!(o, opts)
          when not is_list(opts),
          do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["opts cannot be nil; opts must be a list"])

      def save!(o, opts) do
        kep_timestamp = Keyword.get(opts, :kep_timestamp, false)

        o =
          if kep_timestamp do
            o
          else
            now = System.system_time(:second)
            {:ok, o} = put!(o, :timestamp, now)
            o
          end

        MnesiarRepo.save!(@table_name, o)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def save!(o, opts, async, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def save!(o, opts, async, rescue_func, rescue_func_args, module) do
        if not is_nil(@persistent_schema) do
          SelfModule.save_persistent!(o, async, rescue_func, rescue_func_args, module)
        end

        SelfModule.save!(o, opts)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def delete!(id) do
        MnesiarRepo.delete!(@table_name, id)
      end

      ##############################################################################
      @doc """
      ### Function

        ```
          alias ApiCore.Db.InMemory.Dbo.User, as: InMemoryUser
          id = 1
          opts = [
            skip_cache_check: true,

            # s1 control pure select from persistent db
            s1_exclude_index_filter: true,
            s1_exclude_opts_filter: false,
            s1_combine_filters_with: :or,

            # s2 control select from persistent db if come record was found in cache
            s2_exclude_index_filter: false,
            s2_exclude_opts_filter: false,
            s2_combine_filters_with: :sdfsf,
            combine_filters_with: :and,
            filters: [
              %{
                or: [
                  {:username, :eq, username},
                  {:cell_phone, :eq, username},
                  {:e_mail, :eq, username}
                ]
              }
            ]
          ]

          InMemoryUser.get_by_id!(id, opts)
        ```
      """
      def get_by_id!(id, opts \\ [])

      def get_by_id!(id, opts) do
        opts_skip_cache_check = Keyword.get(opts, :skip_cache_check, false)

        result =
          if opts_skip_cache_check do
            {:ok, :CODE_NOTHING_FOUND}
          else
            MnesiarRepo.get_by_id!(@table_name, id)
          end

        opts_filters = Keyword.get(opts, :filters, [])

        opts_s1_exclude_index_filter = Keyword.get(opts, :s1_exclude_index_filter, false)
        opts_s1_exclude_opts_filter = Keyword.get(opts, :s1_exclude_opts_filter, false)
        opts_s1_combine_filters_with = Keyword.get(opts, :s1_combine_filters_with, :and)

        opts_s2_exclude_index_filter = Keyword.get(opts, :s2_exclude_index_filter, false)
        opts_s2_exclude_opts_filter = Keyword.get(opts, :s2_exclude_opts_filter, false)
        opts_s2_combine_filters_with = Keyword.get(opts, :s2_combine_filters_with, :and)

        opts = Keyword.delete(opts, :skip_cache_check)

        opts = Keyword.delete(opts, :filters)

        opts = Keyword.delete(opts, :s1_exclude_index_filter)
        opts = Keyword.delete(opts, :s1_exclude_opts_filter)
        opts = Keyword.delete(opts, :s1_combine_filters_with)

        opts = Keyword.delete(opts, :s2_combine_filters_with)
        opts = Keyword.delete(opts, :s2_exclude_index_filter)
        opts = Keyword.delete(opts, :s2_exclude_opts_filter)

        # TODO: U should check @table_type before set limit
        limit = 1

        # TODO: Please, try to use COND instead IF
        result =
          if result == {:ok, :CODE_NOTHING_FOUND} do
            if is_nil(@persistent_schema) do
              result
            else
              s1_filters =
                cond do
                  opts_s1_exclude_index_filter and opts_s1_exclude_opts_filter ->
                    []

                  not opts_s1_exclude_index_filter and opts_s1_exclude_opts_filter ->
                    [{:id, :eq, id}]

                  opts_s1_exclude_index_filter and not opts_s1_exclude_opts_filter ->
                    opts_filters

                  not opts_s1_exclude_index_filter and not opts_s1_exclude_opts_filter and opts_s1_combine_filters_with in @logical_operators ->
                    Map.put(%{}, opts_s1_combine_filters_with, opts_filters ++ [{:id, :eq, id}])

                  true ->
                    UniError.raise_error!(:CODE_UNEXPECTED_OPTS_COMBINATION_ERROR, ["Unexpected options combination"],
                      s1_exclude_index_filter: opts_s1_exclude_index_filter,
                      s1_exclude_opts_filter: opts_s1_exclude_opts_filter,
                      s1_combine_filters_with: opts_s1_combine_filters_with
                    )
                end

              SelfModule.get_persistent!(s1_filters, limit, opts)
            end
          else
            {:ok, cached_data_ttl} = StateUtils.get_state!(SelfModule, :cached_data_ttl)
            {:ok, items} = result

            items =
              if is_list(items) do
                items
              else
                [items]
              end

            items =
              Enum.reduce(
                items,
                [],
                fn item, accum ->
                  now = System.system_time(:second)

                  {:ok, timestamp} = SelfModule.get!(item, :timestamp)
                  {:ok, id} = SelfModule.get!(item, :id)

                  if is_nil(cached_data_ttl) do
                    accum ++ [item]
                  else
                    if now - timestamp > cached_data_ttl and not is_nil(@persistent_schema) do
                      SelfModule.delete!(id)

                      if is_nil(@persistent_schema) do
                        accum
                      else
                        s2_filters =
                          cond do
                            opts_s2_exclude_index_filter and opts_s2_exclude_opts_filter ->
                              []

                            not opts_s2_exclude_index_filter and opts_s2_exclude_opts_filter ->
                              [{:id, :eq, id}]

                            opts_s2_exclude_index_filter and not opts_s2_exclude_opts_filter ->
                              opts_filters

                            not opts_s2_exclude_index_filter and not opts_s2_exclude_opts_filter and opts_s2_combine_filters_with in @logical_operators ->
                              Map.put(%{}, opts_s2_combine_filters_with, opts_filters ++ [{:id, :eq, id}])

                            true ->
                              UniError.raise_error!(:CODE_UNEXPECTED_OPTS_COMBINATION_ERROR, ["Unexpected options combination"],
                                s2_exclude_index_filter: opts_s2_exclude_index_filter,
                                s2_exclude_opts_filter: opts_s2_exclude_opts_filter,
                                s2_combine_filters_with: opts_s2_combine_filters_with
                              )
                          end

                        {:ok, items2} = SelfModule.get_persistent!(s2_filters, limit, opts)

                        if items2 == :CODE_NOTHING_FOUND do
                          accum
                        else
                          accum ++ items2
                        end
                      end
                    else
                      accum ++ [item]
                    end
                  end
                end
              )

            {:ok, items}
          end
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def read_all!() do
        MnesiarRepo.read_all!(@table_name)
      end

      ##############################################################################
      @doc """
      ### Function
        
        ```
          alias ApiCore.Db.InMemory.Dbo.User, as: InMemoryUser
          username = "system"
          opts = [
            skip_cache_check: false,

            # s1 control pure select from persistent db
            s1_exclude_index_filter: true,
            s1_exclude_opts_filter: false,
            s1_combine_filters_with: :or,

            # s2 control select from persistent db if come record was found in cache
            s2_exclude_index_filter: false,
            s2_exclude_opts_filter: false,
            s2_combine_filters_with: :sdfsf,
            combine_filters_with: :and,
            filters: [
              %{
                or: [
                  {:username, :eq, username},
                  {:cell_phone, :eq, username},
                  {:e_mail, :eq, username}
                ]
              }
            ]
          ]

          InMemoryUser.find_with_index!(:username, username, opts)
        ```
      """
      def find_with_index!(index, value, opts \\ [])

      def find_with_index!(index, value, opts) do
        opts_skip_cache_check = Keyword.get(opts, :skip_cache_check, false)

        result =
          if opts_skip_cache_check do
            {:ok, :CODE_NOTHING_FOUND}
          else
            MnesiarRepo.find_with_index!(@table_name, index, value)
          end

        opts_filters = Keyword.get(opts, :filters, [])

        opts_s1_exclude_index_filter = Keyword.get(opts, :s1_exclude_index_filter, false)
        opts_s1_exclude_opts_filter = Keyword.get(opts, :s1_exclude_opts_filter, false)
        opts_s1_combine_filters_with = Keyword.get(opts, :s1_combine_filters_with, :and)

        opts_s2_exclude_index_filter = Keyword.get(opts, :s2_exclude_index_filter, false)
        opts_s2_exclude_opts_filter = Keyword.get(opts, :s2_exclude_opts_filter, false)
        opts_s2_combine_filters_with = Keyword.get(opts, :s2_combine_filters_with, :and)

        opts = Keyword.delete(opts, :filters)

        opts = Keyword.delete(opts, :s1_exclude_index_filter)
        opts = Keyword.delete(opts, :s1_exclude_opts_filter)
        opts = Keyword.delete(opts, :s1_combine_filters_with)

        opts = Keyword.delete(opts, :s2_combine_filters_with)
        opts = Keyword.delete(opts, :s2_exclude_index_filter)
        opts = Keyword.delete(opts, :s2_exclude_opts_filter)

        # TODO: U should check @table_type before set limit
        limit = 1

        # TODO: Please, try to use COND instead IF
        result =
          if result == {:ok, :CODE_NOTHING_FOUND} do
            if is_nil(@persistent_schema) do
              result
            else
              s1_filters =
                cond do
                  opts_s1_exclude_index_filter and opts_s1_exclude_opts_filter ->
                    []

                  not opts_s1_exclude_index_filter and opts_s1_exclude_opts_filter ->
                    [{index, :eq, value}]

                  opts_s1_exclude_index_filter and not opts_s1_exclude_opts_filter ->
                    opts_filters

                  not opts_s1_exclude_index_filter and not opts_s1_exclude_opts_filter and opts_s1_combine_filters_with in @logical_operators ->
                    Map.put(%{}, opts_s1_combine_filters_with, opts_filters ++ [{index, :eq, value}])

                  true ->
                    UniError.raise_error!(:CODE_UNEXPECTED_OPTS_COMBINATION_ERROR, ["Unexpected options combination"],
                      s1_exclude_index_filter: opts_s1_exclude_index_filter,
                      s1_exclude_opts_filter: opts_s1_exclude_opts_filter,
                      s1_combine_filters_with: opts_s1_combine_filters_with
                    )
                end

              SelfModule.get_persistent!(s1_filters, limit, opts)
            end
          else
            {:ok, cached_data_ttl} = StateUtils.get_state!(SelfModule, :cached_data_ttl)
            {:ok, items} = result

            items =
              if is_list(items) do
                items
              else
                [items]
              end

            items =
              Enum.reduce(
                items,
                [],
                fn item, accum ->
                  now = System.system_time(:second)

                  {:ok, timestamp} = SelfModule.get!(item, :timestamp)
                  {:ok, id} = SelfModule.get!(item, :id)

                  if is_nil(cached_data_ttl) do
                    accum ++ [item]
                  else
                    if now - timestamp > cached_data_ttl and not is_nil(@persistent_schema) do
                      SelfModule.delete!(id)

                      if is_nil(@persistent_schema) do
                        accum
                      else
                        s2_filters =
                          cond do
                            opts_s2_exclude_index_filter and opts_s2_exclude_opts_filter ->
                              []

                            not opts_s2_exclude_index_filter and opts_s2_exclude_opts_filter ->
                              [{:id, :eq, id}]

                            opts_s2_exclude_index_filter and not opts_s2_exclude_opts_filter ->
                              opts_filters

                            not opts_s2_exclude_index_filter and not opts_s2_exclude_opts_filter and opts_s2_combine_filters_with in @logical_operators ->
                              Map.put(%{}, opts_s2_combine_filters_with, opts_filters ++ [{:id, :eq, id}])

                            true ->
                              UniError.raise_error!(:CODE_UNEXPECTED_OPTS_COMBINATION_ERROR, ["Unexpected options combination"],
                                s2_exclude_index_filter: opts_s2_exclude_index_filter,
                                s2_exclude_opts_filter: opts_s2_exclude_opts_filter,
                                s2_combine_filters_with: opts_s2_combine_filters_with
                              )
                          end

                        {:ok, items2} = SelfModule.get_persistent!(s2_filters, limit, opts)

                        if items2 == :CODE_NOTHING_FOUND do
                          accum
                        else
                          accum ++ items2
                        end
                      end
                    else
                      accum ++ [item]
                    end
                  end
                end
              )

            {:ok, items}
          end
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def find_with_match_object!(pattern \\ {:_})

      def find_with_match_object!(pattern) do
        MnesiarRepo.find_with_match_object!(@table_name, pattern)
      end

      ##############################################################################
      @doc """
      ### Function
      """
      def table_info!(table, key) do
        MnesiarRepo.table_info!(@table_name, key)
      end

      defoverridable MnesiarRepo
    end
  end

  ##############################################################################
  ##############################################################################
end
