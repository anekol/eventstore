defmodule EventStore.Tasks.Init do
  @moduledoc """
  Task to initalize the EventStore database
  """

  import EventStore.Tasks.Output

  alias EventStore.Config
  alias EventStore.Storage.Database
  alias EventStore.Storage.Initializer

  @doc """
  Runs task

  ## Parameters

    - config: the parsed EventStore config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task
    - quiet: set to `true` to silence output

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)

    if Database.exists?(config) do
      schema = Keyword.fetch!(config, :schema)

      {:ok, conn} =
        config
        |> Config.default_postgrex_opts()
        |> Postgrex.start_link()

      query = """
        SELECT EXISTS (
          SELECT 1
          FROM   information_schema.tables
          WHERE  table_schema = $1
          AND    table_name = 'events'
        )
      """

      case Postgrex.query!(conn, query, [schema]) do
        %{rows: [[true]]} ->
          write_info("The EventStore #{eventstore(config)} has already been initialized.", opts)

        %{rows: [[false]]} ->
          Initializer.run!(conn, config)

          write_info("The EventStore #{eventstore(config)} has been initialized.", opts)
      end

      true = Process.unlink(conn)
      true = Process.exit(conn, :shutdown)

      :ok
    else
      raise_msg(
        "The EventStore #{eventstore(config)} has not been created.",
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
