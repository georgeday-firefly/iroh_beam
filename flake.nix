{
  description = "iroh_beam development shell (OTP 29, Elixir 1.20, Rust)";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs = { nixpkgs, ... }:
    let
      systems = [ "aarch64-darwin" "aarch64-linux" "x86_64-linux" ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = with pkgs; [
            beam29Packages.erlang
            beam29Packages.elixir_1_20
            rustc
            cargo
            clippy
            rustfmt
          ];
        };
      });
    };
}
