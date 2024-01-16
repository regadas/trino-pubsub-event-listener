{ pkgs ? import <nixpkgs-unstable> { } }:

pkgs.mkShell {
  buildInputs = [
    (pkgs.gradle.override { java = pkgs.jdk21; })
    pkgs.jdk21
  ];
}
