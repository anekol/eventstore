defmodule EventStore.Tasks.Create do
  @moduledoc """
  Task to create the EventStore
  """

  import EventStore.Tasks.Output

  alias EventStore.Storage.Database
  alias EventStore.Storage.Schema

  @doc """
  Runs database and schema create task.

  ## Parameters

    - config: the parsed EventStore config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task
    - quiet: set to `true` to silence output

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)

    case Database.create(config) do
      :ok ->
        write_info("The EventStore #{eventstore(config)} has been created.", opts)

      {:error, :already_up} ->
        if config[:schema] == "public",
          do: write_info("The EventStore #{eventstore(config)} already exists.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStore #{eventstore(config)} couldn't be created, reason given: #{inspect(term)}.",
          opts
        )
    end

    case Schema.create(config) do
      :ok ->
        write_info(
          "The EventStore #{eventstore(config)} has been created.",
          opts
        )

      {:error, :already_up} ->
        if config[:schema] != "public",
          do: write_info("The EventStore #{eventstore(config)} already exists.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStore #{eventstore(config)} couldn't be created, reason given: #{inspect(term)}.",
          opts
        )
    end
  end

  defp eventstore(config) do
    case(config[:schema]) do
      "public" -> "database \"#{config[:database]}\""
      _ -> "database \"#{config[:database]} schema \"#{config[:schema]}\""
    end
  end
end
