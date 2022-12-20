defmodule Mnesiar.Repo do
  ##############################################################################
  ##############################################################################
  @moduledoc """

  """

  use Utils

  alias :mnesia, as: Mnesia

  @schema_storage_types [:disc_copies, :ram_copies]
  @entity_storage_types [:disc_copies, :ram_copies, :disc_only_copies]
  @entity_types [:set, :ordered_set, :bag]
  @change_config_opts [:extra_db_nodes, :dc_dump_limit]

  ##############################################################################
  @doc """

  """
  def state!() do
    result = Mnesia.system_info(:is_running)

    result =
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

  """
  def create_schema!(create_schema_timeout, nodes) do
    Logger.info("[#{Node.self()}] I will try create schema in in-memory DB on nodes #{inspect(nodes)}")

    result = Mnesia.create_schema(nodes)

    result =
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
        value ->
          value

        {:error, reason} ->
          UniError.raise_error!(:CODE_TABLE_INFO_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, key: key)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_TABLE_INFO_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, key: key)
      end

    {:ok, result}
  end

  ##############################################################################
  @doc """

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
            result = raise_if_empty!(item, :atom, "Wrong index value")

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

        {:aborted, {:already_exists, table_name}} ->
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

        {:ok, pid} = StateUtils.init_state!(module, et)
      end
    )

    :ok
  end

  ##############################################################################
  @doc """

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

  """
  def ensure_db_is_empty!() do
    {:ok, local_tables} = system_info!(:local_tables)

    {:ok, local_tables == [:schema]}
  end

  ##############################################################################
  @doc """

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

  """
  def system_info!(option)
      when is_nil(option) or not is_atom(option),
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["option cannot be nil; option must be an atom"])

  def system_info!(key) do
    result = Mnesia.system_info(key)

    result =
      case result do
        value ->
          value

        {:error, reason} ->
          UniError.raise_error!(:CODE_SYSTEM_INFO_IN_MEMORY_DB_ERROR, ["Error occurred while process operation in-memory DB"], previous: reason, key: key)

        {:aborted, reason} ->
          UniError.raise_error!(:CODE_SYSTEM_INFO_IN_MEMORY_DB_ABORTED_ERROR, ["Aborted occurred while process operation in-memory DB"], previous: reason, key: key)
      end

    {:ok, result}
  end

  ##############################################################################
  @doc """

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

  """
  def add_table_copy_of_storage_type!(table, node, storage_type)
      when is_nil(table) or is_nil(node) or is_nil(storage_type) or not is_atom(table) or
             not is_atom(node) or not is_atom(storage_type) or
             storage_type not in @entity_storage_types,
      do: UniError.raise_error!(:CODE_WRONG_FUNCTION_ARGUMENT_ERROR, ["table, node, storage_type cannot be nil; table, node, storage_type must be an atom; storage_type must be on of #{inspect(@entity_storage_types)}"])

  def add_table_copy_of_storage_type!(table, node, storage_type) do
    result = Mnesia.add_table_copy(table, node, storage_type)

    result =
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

        {:aborted, {:already_exists, table_name, ret_node, ret_type}} ->
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
          :ok

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
        result =
          Enum.reduce(
            list,
            0,
            fn item, accum ->
              delete_object!(table, item)
              accum = accum + 1
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

  """
  @callback load() :: term
  @callback get_by_id!(id :: any) :: term
  @callback get_persistent!(opt :: any) :: term
  @callback save_persistent!(o :: any, async :: any, rescue_func :: any, rescue_func_args :: any, module :: any) :: term
  @callback prepare!(o :: any) :: term

  @optional_callbacks load: 0, get_by_id!: 1, get_persistent!: 1, save_persistent!: 5

  ##############################################################################
  @doc """

  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use Utils

      import Record, only: [defrecord: 3]
      import Ecto.Query, only: [from: 2]

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

      ##############################################################################
      @doc """

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

      """
      def get_table_name() do
        {:ok, @table_name}
      end

      ##############################################################################
      @doc """

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

      """
      def get!(record, key) do
        MnesiarRepo.get!(@attributes, record, key)
      end

      ##############################################################################
      @doc """

      """
      def put!(record, key, value) do
        MnesiarRepo.put!(@attributes, record, key, value)
      end

      ##############################################################################
      @doc """

      """
      def record_to_map(record) do
        MnesiarRepo.record_to_map(@attributes, record)
      end

      ##############################################################################
      @doc """

      """
      def map_to_record(map) do
        MnesiarRepo.map_to_record(@attributes, @record_name, map)
      end

      ##############################################################################
      @doc """

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

      """
      def add_table_copy_of_storage_type!(node, type) do
        MnesiarRepo.add_table_copy_of_storage_type!(@table_name, node, type)
      end

      ##############################################################################
      @doc """

      """
      def change_tables_copies_type!(node, type) do
        MnesiarRepo.change_table_copy_storage_type!(@table_name, node, type)
      end

      ###########################################################################
      @doc """

      """
      def save_persistent!(o, async \\ false, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def save_persistent!(o, async, rescue_func, rescue_func_args, module) do
        if is_nil(@persistent_schema) do
          UniError.raise_error!(:CODE_PERSISTENT_SCHEMA_IS_NIL_MNESIAR_ERROR, ["Persistent schema is nil"], mnesiar_repo: SelfModule)
        end

        {:ok, map} = SelfModule.record_to_map(o)
        map = Map.delete(map, :roles)

        # TODO: Here some manipulations with data. And save in persistent. Maybe in several linked transactions
        # @persistent_schema

        @persistent_schema.update!(map, async, rescue_func, rescue_func_args, module)

        {:ok, o}
      end

      ##############################################################################
      @doc """

      """
      def save!(o) do
        now = System.system_time(:second)
        {:ok, o} = put!(o, :timestamp, now)

        MnesiarRepo.save!(@table_name, o)
      end

      ##############################################################################
      @doc """

      """
      def save!(o, async, rescue_func \\ nil, rescue_func_args \\ [], module \\ nil)

      def save!(o, async, rescue_func, rescue_func_args, module) do
        if not is_nil(@persistent_schema) do
          SelfModule.save_persistent!(o, async, rescue_func, rescue_func_args, module)
        end

        MnesiarRepo.save!(@table_name, o)
      end

      ##############################################################################
      @doc """

      """
      def delete!(id) do
        MnesiarRepo.delete!(@table_name, id)
      end

      ###########################################################################
      @doc """

      """
      @impl true
      def get_persistent!(id: id) do
        if is_nil(@persistent_schema) do
          UniError.raise_error!(:CODE_PERSISTENT_SCHEMA_IS_NIL_MNESIAR_ERROR, ["Persistent schema is nil"], mnesiar_repo: SelfModule)
        end

        query =
          from(
            o in @persistent_schema,
            where: o.id == ^id,
            limit: 1,
            select: o
          )

        opts = []
        result = @persistent_schema.get_by_query!(query, opts)

        if result == {:ok, :CODE_NOTHING_FOUND} do
          result
        else
          {:ok, [item]} = result
          item = Map.from_struct(item)

          {:ok, item} = SelfModule.map_to_record(item)
          {:ok, item} = SelfModule.prepare!(item)
          SelfModule.save!(item)

          {:ok, item}
        end
      end

      ##############################################################################
      @doc """

      """
      @impl true
      def get_by_id!(id) do
        result = MnesiarRepo.get_by_id!(@table_name, id)

        # TODO: Please, try to use COND instead IF
        result =
          if result == {:ok, :CODE_NOTHING_FOUND} do
            if is_nil(@persistent_schema) do
              result
            else
              SelfModule.get_persistent!(id: id)
            end
          else
            {:ok, cached_data_ttl} = StateUtils.get_state!(SelfModule, :cached_data_ttl)
            now = System.system_time(:second)
            {:ok, item} = result

            {:ok, timestamp} = SelfModule.get!(item, :timestamp)

            if is_nil(cached_data_ttl) do
              result
            else
              if now - timestamp > cached_data_ttl and not is_nil(@persistent_schema) do
                SelfModule.get_persistent!(id: id)
              else
                result
              end
            end
          end
      end

      ##############################################################################
      @doc """

      """
      def read_all!() do
        MnesiarRepo.read_all!(@table_name)
      end

      ##############################################################################
      @doc """

      """
      def find_with_index!(index, value) do
        MnesiarRepo.find_with_index!(@table_name, index, value)
      end

      ##############################################################################
      @doc """

      """
      def find_with_match_object!(pattern \\ {:_})

      def find_with_match_object!(pattern) do
        MnesiarRepo.find_with_match_object!(@table_name, pattern)
      end

      def table_info!(table, key) do
        MnesiarRepo.table_info!(@table_name, key)
      end

      @impl true
      def load() do
        {:ok, table_name} = get_table_name()

        Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] I will load data to table #{table_name} in in-memory DB")

        Logger.info("[#{inspect(__MODULE__)}][#{inspect(__ENV__.function)}] Data loaded to table #{table_name} in in-memory DB successfully")

        :ok
      end

      defoverridable MnesiarRepo
    end
  end

  ##############################################################################
  ##############################################################################
end
