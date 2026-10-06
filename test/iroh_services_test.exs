defmodule IrohBeam.IrohServicesTest do
  use IrohBeam.FixtureCase, async: false

  import IrohBeam.Eventually

  alias IrohBeam.{Endpoint, Error}

  @alpn "iroh-beam/iroh-services-test/1"

  test "iroh_services settings are validated before binding" do
    for options <- [
          [relays: ["https://relay.example"]],
          [api_secret_file: 1],
          [api_secret_file: "x", relays: [1]],
          [api_secret_file: "x", diagnostics: "yes"]
        ] do
      assert {:error, %Error{category: :invalid_argument, message: message}} =
               Endpoint.start_link(alpns: [@alpn], network: {:iroh_services, options})

      assert message =~ "api_secret_file"
    end
  end

  test "an unreadable or malformed API secret fails the bind without disclosing it", %{
    tmp_dir: tmp_dir
  } do
    missing = Path.join(tmp_dir, "missing-secret")

    assert {:error, %Error{operation: :endpoint_bind, message: message}} =
             Endpoint.start_link(
               alpns: [@alpn],
               network: {:iroh_services, api_secret_file: missing}
             )

    assert message =~ "could not be read"

    malformed = Path.join(tmp_dir, "malformed-secret")
    File.write!(malformed, "not-an-iroh-services-secret\n")

    assert {:error, %Error{operation: :endpoint_bind, message: message} = error} =
             Endpoint.start_link(
               alpns: [@alpn],
               network: {:iroh_services, api_secret_file: malformed}
             )

    assert message =~ "invalid"
    refute inspect(error) =~ "not-an-iroh-services-secret"
  end

  # Needs a real project: IROH_SERVICES_API_SECRET_FILE, and optionally
  # IROH_SERVICES_RELAYS as comma-separated relay URLs.
  @tag :public_network
  test "an iroh_services endpoint comes online on the project relays and reports" do
    relays =
      System.get_env("IROH_SERVICES_RELAYS", "") |> String.split(",", trim: true)

    {:ok, endpoint} =
      Endpoint.start_link(
        alpns: [@alpn],
        network:
          {:iroh_services,
           api_secret_file: System.fetch_env!("IROH_SERVICES_API_SECRET_FILE"),
           relays: relays,
           name: "iroh-beam-test-#{System.unique_integer([:positive])}",
           diagnostics: true}
      )

    assert :ok = Endpoint.await_online(endpoint, 15_000)

    assert_eventually(
      fn -> match?({:ok, %{services_reporting?: true}}, Endpoint.status(endpoint)) end,
      15_000
    )

    assert {:ok, %{profile: :iroh_services}} = Endpoint.status(endpoint)
    assert :ok = Endpoint.close(endpoint)
  end
end
