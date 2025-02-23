defmodule EventStore.Storage.Schema do
  @moduledoc false

  alias EventStore.Storage.Database

  def create(config) do
    schema = Keyword.fetch!(config, :schema)

    case Database.execute(config, ~s(CREATE SCHEMA "#{schema}")) do
      :ok ->
        :ok

      {:error, %{postgres: %{code: :duplicate_schema}}} ->
        {:error, :already_up}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  def drop(config) do
    schema = Keyword.fetch!(config, :schema)

    if Database.exists?(config) do
      case Database.execute(config, ~s(DROP SCHEMA "#{schema}" CASCADE;)) do
        :ok ->
          :ok

        {:error, %{postgres: %{code: :invalid_schema_name}}} ->
          {:error, :already_down}

        {:error, error} ->
          {:error, Exception.message(error)}
      end
    else
      {:error, :already_down}
    end
  end
end
