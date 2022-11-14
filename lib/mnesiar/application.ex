defmodule Mnesiar.Application do
  ##############################################################################
  ##############################################################################
  @moduledoc """

  """
  use Application
  use Utils

  alias Mnesiar, as: Mnesiar

  ##############################################################################
  @doc """
  # get_opts.
  """
  defp get_opts do
    result = [
      strategy: :one_for_one,
      name: Mnesiar.Supervisor
    ]

    {:ok, result}
  end

  ##############################################################################
  @doc """
  # get_children!
  """
  defp get_children! do
    {:ok, config} = Mnesiar.get_config!()

    result = []

    {:ok, result}
  end

  ##############################################################################
  @doc """
  # Start application.
  """
  def start(_type, _args) do
    {:ok, children} = get_children!()
    {:ok, opts} = get_opts()

    Supervisor.start_link(children, opts)
  end

  ##############################################################################
  ##############################################################################
end
