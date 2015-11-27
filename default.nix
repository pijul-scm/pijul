with (import <nixpkgs> {});

stdenv.mkDerivation rec {
  name = "rust-pijul-${version}";
  version = "0.0";
  src = ./.;

  buildInputs = [ rustPlatform.rustc rustPlatform.cargo lmdb openssl libssh2 zlib pkgconfig ];
  
  depsSha256 = "0g8hh29dxsq81h2zjri67jzmbg660ja3hif776vn341zknmq1n17";
  
}
