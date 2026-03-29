/**
 * Rspack Configuration for Pylance Development Builds
 *
 * DIFFERENCES FROM WEBPACK CONFIG:
 * =================================
 * 1. cacheConfig: Not imported - the webpack helper function returns undefined (caching is disabled)
 * 2. tsconfigResolveAliases: Not used - aliases are manually defined in resolve.alias for clarity
 * 3. All functionality from webpack config is preserved, including:
 *    - Deduplication of LSP packages (via RspackDeduplicationPlugin)
 *    - Browser process polyfill (browserProcess in ProvidePlugin and fallback)
 *    - Static asset copying (CopyRspackPlugin)
 *    - Copilot bundle generation
 *
 * OBFUSCATION NOTES:
 * ==================
 * This configuration uses Rspack's built-in SwcJsMinimizerRspackPlugin for code obfuscation
 * in production mode. While not as aggressive as webpack-obfuscator, it provides:
 *
 * 1. Aggressive minification and mangling (toplevel, classnames, function names)
 * 2. Dead code elimination and tree shaking
 * 3. Multiple compression passes for better optimization
 *
 * The obfuscation is ONLY enabled when mode === 'production'.
 *
 * IMPORTANT: For true production builds with maximum obfuscation matching the webpack
 * configuration, continue using webpack.config.js with webpack-obfuscator.
 *
 * To enable obfuscation in rspack builds:
 * - Change the module.exports at the bottom to use mode: 'production'
 * - Run: npm run rspack -- --mode production
 *
 * Obfuscation comparison:
 * - webpack-obfuscator: Control flow flattening, string array encoding, dead code injection
 * - SwcJsMinimizerRspackPlugin: Aggressive minification/mangling only
 *
 * For development builds (mode: 'development'), obfuscation is disabled for faster builds.
 */

const path = require('path');
const rspack = require('@rspack/core');
const { RspackDeduplicationPlugin } = require('../../build/rspack-deduplication-plugin');

const outPath = path.resolve(__dirname, 'dist');
const packages = path.resolve(__dirname, '..');

const typeshedFallback = path.resolve(packages, 'pyright', 'packages', 'pyright-internal', 'typeshed-fallback');
const bundled = path.resolve(packages, 'pylance-internal', 'bundled');
const rustIndexerBin = path.resolve(__dirname, 'bundled', 'bin');
const schemas = path.resolve(packages, 'pyright', 'packages', 'vscode-pyright', 'schemas');
const scripts = path.resolve(packages, 'pylance-internal', 'stub-generation');
const browserProcess = path.resolve(packages, 'pylance-internal', 'src', 'browser', 'browserProcess.ts');

const { distDir: onnxDir } = require(path.resolve(__dirname, '../pylance-internal/build/findonnx'));
const { wasm: treeSitterWasm } = require(path.resolve(__dirname, '../pylance-internal/build/findTreeSitter'));

// Import copilot config and redirect output to vscode-pylance dist folder
const copilotConfig = require('../pylance-copilot/rspack.server.config.js');
const copilotModifiedConfig = (env, argv) => {
    const config = copilotConfig(env, argv);
    // Override output path and source map template for correct debugging from vscode-pylance/dist
    config.output = {
        ...config.output,
        // Output bundles and source maps to vscode-pylance/dist folder
        path: outPath,
        // Fix source map paths: from vscode-pylance/dist, go up to packages, then into pylance-copilot
        devtoolModuleFilenameTemplate:
            argv.mode === 'development' ? '../../pylance-copilot/[resource-path]' : undefined,
    };
    return config;
};

class PylanceManifestPlugin {
    /**
     * @param {rspack.Compiler} compiler
     */
    apply(compiler) {
        const hookOptions = {
            name: 'PylanceManifestPlugin',
            stage: Infinity,
        };

        compiler.hooks.thisCompilation.tap(hookOptions, (compilation) => {
            compilation.hooks.processAssets.tap(
                {
                    ...hookOptions,
                    stage: rspack.Compilation.PROCESS_ASSETS_STAGE_REPORT,
                },
                (assets) => {
                    const files = Object.keys(assets);

                    // Manually add files added by browserConfig and copilot (built separately)
                    files.push(
                        'browser.extension.bundle.js',
                        'browser.async.bundle.js',
                        'copilot-server.bundle.js',
                        'copilot-background.bundle.js',
                        'copilot-vendor.bundle.js'
                    );

                    const manifest = {
                        files,
                    };

                    compilation.emitAsset('folderIndex.json', new rspack.sources.RawSource(JSON.stringify(manifest)));
                }
            );
        });
    }
}

/** @type {(env: any, argv: { mode: 'production' | 'development' | 'none' }) => import('@rspack/core').Configuration} */
const nodeConfig = (env, { mode }) => {
    /** @type {import('@rspack/core').Configuration['plugins']} */
    const plugins = [
        new RspackDeduplicationPlugin(),
        new rspack.CopyRspackPlugin({
            patterns: [
                { from: `${onnxDir}/ort-wasm.wasm`, to: '[name][ext]' },
                { from: typeshedFallback, to: 'typeshed-fallback' },
                { from: bundled, to: 'bundled' },
                { from: treeSitterWasm, to: 'bundled/wasm' },
                { from: schemas, to: 'schemas' },
                { from: scripts, to: 'stub-generation' },
                // Copy Rust indexer binaries (if they exist)
                { from: rustIndexerBin, to: 'bundled/bin', noErrorOnMissing: true },
            ],
        }),
        new rspack.DefinePlugin({
            BUILD_DEFS: {
                DISABLE_WEBGL: true,
                DISABLE_WASM: false,
                DISABLE_WASM_THREAD: true,
            },
        }),
        new PylanceManifestPlugin(),
    ];

    return {
        context: __dirname,
        entry: {
            extension: path.resolve(__dirname, 'src/extension.ts'),
            server: path.resolve(__dirname, 'src/serverMain.ts'),
            typeServer: path.resolve(__dirname, '../type-server/src/node/nodeMain.ts'),
            mainSandboxWorker: path.resolve(__dirname, 'src/mainSandboxWorker.ts'),
            threadSandboxWorker: path.resolve(__dirname, 'src/threadSandboxWorker.ts'),
        },
        target: 'node',
        output: {
            filename: '[name].bundle.js',
            path: outPath,
            libraryTarget: 'commonjs2',
            devtoolModuleFilenameTemplate: mode === 'development' ? '../[resource-path]' : undefined,
            // Note: Removed clean option - with multi-config, rspack may clean in parallel
            // causing browser files to be removed. Clean should be done externally if needed.
        },
        externalsType: 'commonjs',
        devtool: mode === 'development' ? 'source-map' : undefined,
        stats: {
            all: false,
            errors: true,
            warnings: true,
            publicPath: true,
            timings: true,
            assets: true,
            chunks: true,
            modules: false,
            entrypoints: true,
            colors: true,
        },
        resolve: {
            extensions: ['.tsx', '.ts', '.js'],
            alias: {
                'pylance-internal': path.resolve(__dirname, '../pylance-internal/src'),
                'pyright-internal': path.resolve(__dirname, '../pyright/packages/pyright-internal/src'),
                'type-server': path.resolve(__dirname, '../type-server/src'),
                'pylance-copilot': path.resolve(__dirname, '../pylance-copilot/src'),
            },
            // Rspack needs these modules configured
            modules: ['node_modules', path.resolve(__dirname, '../')],
        },
        externals: [
            // Node built-ins should be external
            function ({ request }, callback) {
                const nodeBuiltins = [
                    'path',
                    'fs',
                    'os',
                    'crypto',
                    'v8',
                    'worker_threads',
                    'http',
                    'https',
                    'net',
                    'child_process',
                    'stream',
                    'util',
                    'events',
                    'buffer',
                    'url',
                    'querystring',
                    'assert',
                    'zlib',
                    'tls',
                    'dns',
                    'dgram',
                    'readline',
                    'repl',
                    'vm',
                    'domain',
                    'constants',
                    'process',
                    'module',
                    'cluster',
                    'punycode',
                    'string_decoder',
                    'timers',
                    'tty',
                    'async_hooks',
                    'perf_hooks',
                ];
                // Check if request starts with node: prefix or is a built-in
                const moduleName = request?.startsWith('node:') ? request.slice(5) : request;
                if (moduleName && nodeBuiltins.includes(moduleName)) {
                    return callback(null, `commonjs ${moduleName}`);
                }
                callback();
            },
            {
                vscode: 'commonjs vscode',
                fsevents: 'commonjs2 fsevents',
                pprof: 'commonjs2 pprof',

                // Explicitly add path as it's not being caught by the function
                path: 'commonjs path',

                // These dependencies are ignored because we don't use them, and App Insights has try-catch protecting their loading if they don't exist
                'applicationinsights-native-metrics': 'commonjs applicationinsights-native-metrics',
                '@opentelemetry/tracing': 'commonjs @opentelemetry/tracing',
                '@opentelemetry/instrumentation': 'commonjs @opentelemetry/instrumentation',
                '@azure/functions-core': 'commonjs @azure/functions-core',
                '@azure/opentelemetry-instrumentation-azure-sdk':
                    'commonjs @azure/opentelemetry-instrumentation-azure-sdk',
            },
        ],
        module: {
            rules: [
                {
                    test: /\.tsx?$/,
                    use: {
                        loader: 'builtin:swc-loader',
                        options: {
                            jsc: {
                                parser: {
                                    syntax: 'typescript',
                                    decorators: true,
                                },
                                target: 'es2019',
                                transform: {
                                    decoratorMetadata: true,
                                    legacyDecorator: true,
                                },
                            },
                        },
                    },
                    type: 'javascript/auto',
                },
            ],
        },
        optimization: {
            usedExports: true,
            minimize: mode === 'production',
            minimizer:
                mode === 'production'
                    ? [
                          // Apply full obfuscation (compress + mangle) to normal chunks only
                          new rspack.SwcJsMinimizerRspackPlugin({
                              test: /^(?!.*(?:vendor|pyright)\.bundle\.js$).*\.js$/,
                              minimizerOptions: {
                                  compress: {
                                      passes: 2,
                                      dead_code: true,
                                      drop_console: false,
                                      drop_debugger: true,
                                      unused: true,
                                  },
                                  mangle: {
                                      toplevel: true,
                                      keep_classnames: false,
                                      keep_fnames: false,
                                  },
                              },
                          }),
                          // Apply only compression (no mangle) to vendor and pyright chunks
                          new rspack.SwcJsMinimizerRspackPlugin({
                              test: /(?:vendor|pyright)\.bundle\.js$/,
                              minimizerOptions: {
                                  compress: {
                                      passes: 2,
                                      dead_code: true,
                                      drop_console: false,
                                      drop_debugger: true,
                                      unused: true,
                                  },
                                  mangle: false,
                              },
                          }),
                      ]
                    : [],
            splitChunks: {
                cacheGroups: {
                    defaultVendors: {
                        name: 'vendor',
                        test: /[\\/]node_modules[\\/]/,
                        chunks: 'all',
                        priority: -10,
                    },
                    pyright: {
                        name: 'pyright',
                        chunks: 'all',
                        test: /[\\/]pyright-internal[\\/]/,
                        priority: -20,
                    },
                },
            },
        },
        plugins,
    };
};

/**@type {(env: any, argv: { mode: 'production' | 'development' | 'none' }) => import('@rspack/core').Configuration}*/
const browserConfig = (env, { mode }) => {
    console.log('browserConfig: Creating configuration for mode:', mode);
    /** @type {import('@rspack/core').Configuration['plugins']} */
    const plugins = [
        new RspackDeduplicationPlugin(),
        new rspack.ProvidePlugin({
            process: browserProcess,
            Buffer: ['buffer', 'Buffer'],
        }),
        new rspack.optimize.LimitChunkCountPlugin({
            maxChunks: 1,
        }),
    ];

    return {
        context: __dirname,
        entry: {
            extension: {
                import: path.resolve(__dirname, 'src/browserExtension.ts'),
                library: {
                    type: 'commonjs2',
                },
            },
            async: {
                import: path.resolve(__dirname, 'src/browserAsync.ts'),
                // The server is loaded in a Worker and is not a library. Attempting
                // to export (e.g. via commonjs exports) will crash.
            },
            'tests/web/index': {
                import: path.resolve(__dirname, 'src/tests/web/index.ts'), // For web tests
                library: {
                    type: 'commonjs2',
                },
            },
        },
        target: 'webworker',
        output: {
            filename: 'browser.[name].bundle.js',
            path: outPath,
            devtoolModuleFilenameTemplate:
                mode === 'development'
                    ? (info) => {
                          if (info.absoluteResourcePath.endsWith('.ts')) {
                              return `../${info.resourcePath}`;
                          }
                          return info.absoluteResourcePath;
                      }
                    : undefined,
            // Don't clean - let the node config handle cleaning non-browser files
        },
        name: 'browser', // Add a name to help debug
        devtool: mode === 'development' ? 'source-map' : undefined,
        stats: {
            all: false,
            errors: true,
            warnings: true,
            publicPath: true,
            timings: true,
            assets: true,
            chunks: true,
            modules: false,
            entrypoints: true,
            colors: true,
        },
        resolve: {
            extensions: ['.tsx', '.ts', '.js'],
            alias: {
                'pylance-internal': path.resolve(__dirname, '../pylance-internal/src'),
                'pyright-internal': path.resolve(__dirname, '../pyright/packages/pyright-internal/src'),
                'type-server': path.resolve(__dirname, '../type-server/src'),
                'pylance-copilot': path.resolve(__dirname, '../pylance-copilot/src'),
            },
            // Rspack needs these modules configured
            modules: ['node_modules', path.resolve(__dirname, '../')],
            fallback: {
                buffer: require.resolve('buffer/'), // Used by stream
                events: require.resolve('events/'), // Used by stream
                stream: require.resolve('stream-browserify'),
                path: require.resolve('path-browserify'),
                process: browserProcess,
                // Note: this will make these imports empty objects, not make them fail to resolve.
                crypto: false,
                worker_threads: false,
                child_process: false,
                v8: false,
                os: false,
                fs: false,
                http: false,
                net: false,
            },
        },
        externals: [
            // Node built-ins should be external (same as node config)
            function ({ request }, callback) {
                const nodeBuiltins = [
                    'path',
                    'fs',
                    'os',
                    'crypto',
                    'v8',
                    'worker_threads',
                    'http',
                    'https',
                    'net',
                    'child_process',
                    'stream',
                    'util',
                    'events',
                    'buffer',
                    'url',
                    'querystring',
                    'assert',
                    'zlib',
                    'tls',
                    'dns',
                    'dgram',
                    'readline',
                    'repl',
                    'vm',
                    'domain',
                    'constants',
                    'process',
                    'module',
                    'cluster',
                    'punycode',
                    'string_decoder',
                    'timers',
                    'tty',
                    'async_hooks',
                    'perf_hooks',
                ];
                // Check if request starts with node: prefix or is a built-in
                const moduleName = request?.startsWith('node:') ? request.slice(5) : request;
                if (moduleName && nodeBuiltins.includes(moduleName)) {
                    return callback(null, `commonjs ${moduleName}`);
                }
                callback();
            },
            {
                vscode: 'commonjs vscode',

                // Explicitly add path as it's not being caught by the function
                path: 'commonjs path',

                // These dependencies are ignored because we don't use them, and App Insights has try-catch protecting their loading if they don't exist
                'applicationinsights-native-metrics': 'commonjs applicationinsights-native-metrics',
                '@opentelemetry/tracing': 'commonjs @opentelemetry/tracing',
                '@opentelemetry/instrumentation': 'commonjs @opentelemetry/instrumentation',
                '@azure/functions-core': 'commonjs @azure/functions-core',
                '@azure/opentelemetry-instrumentation-azure-sdk':
                    'commonjs @azure/opentelemetry-instrumentation-azure-sdk',
            },
        ],
        module: {
            rules: [
                {
                    test: /\.tsx?$/,
                    use: {
                        loader: 'builtin:swc-loader',
                        options: {
                            jsc: {
                                parser: {
                                    syntax: 'typescript',
                                    decorators: true,
                                },
                                target: 'es2019',
                                transform: {
                                    decoratorMetadata: true,
                                    legacyDecorator: true,
                                },
                            },
                        },
                    },
                    type: 'javascript/auto',
                },
            ],
        },
        performance: {
            hints: false, // Silence rspack's warnings about the size of our chunks.
        },
        plugins,
    };
};

// For rspack, multi-config arrays execute in order
// Browser config must run FIRST, then node config, then copilot config
// Export as a function so rspack CLI can pass the mode from command line
module.exports = (env = {}, argv = {}) => {
    const mode = argv.mode || 'development';
    return [browserConfig(env, { mode }), nodeConfig(env, { mode }), copilotModifiedConfig(env, { mode })];
};
