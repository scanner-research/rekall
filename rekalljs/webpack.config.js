"use strict"

const pkg = require('./package.json');

module.exports = {
  entry: "./src/rekall.tsx",

  output: {
    filename: "bundle.js",
    path: __dirname + "/dist",
    libraryTarget: 'umd',
    library: pkg.name,
    umdNamedDefine: true
  },

  devtool: "source-map",

  resolve: {
    modules: ['node_modules', 'src'],
    extensions: [".ts", ".tsx", ".js"]
  },

  module: {
    rules: [
      // All files with a '.ts' or '.tsx' extension will be handled by 'awesome-typescript-loader'.
      { test: /\.tsx?$/, loader: "awesome-typescript-loader" },

      // All output '.js' files will have any sourcemaps re-processed by 'source-map-loader'.
      { enforce: "pre", test: /\.js$/, loader: "source-map-loader" }
    ]
  }
}
