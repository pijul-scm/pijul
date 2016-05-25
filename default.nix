with (import <nixpkgs> {});

stdenv.mkDerivation rec {
  name = "rust-pijul-${version}";
  version = "0.0";
  src = ./.;

  buildInputs = [ rustPlatform.rustc rustPlatform.cargo openssl libssh zlib pkgconfig rustfmt ];
}
