with (import <nixpkgs> {});

stdenv.mkDerivation rec {
  name = "rust-pijul-${version}";
  version = "0.0";
  src = ./.;

  buildInputs = [ rustUnstable.rustc rustUnstable.cargo lmdb openssl libssh zlib pkgconfig ];
  
}
