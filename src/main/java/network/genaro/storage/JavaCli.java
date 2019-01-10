package network.genaro.storage;

import gnu.getopt.Getopt;
import gnu.getopt.LongOpt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.Console;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import network.genaro.storage.GenaroCallback.GetBucketsCallback;
import network.genaro.storage.GenaroCallback.DeleteBucketCallback;
import network.genaro.storage.GenaroCallback.RenameBucketCallback;
import network.genaro.storage.GenaroCallback.ListFilesCallback;
import network.genaro.storage.GenaroCallback.ListMirrorsCallback;
import network.genaro.storage.GenaroCallback.DeleteFileCallback;
import network.genaro.storage.GenaroCallback.ResolveFileCallback;
import network.genaro.storage.GenaroCallback.StoreFileCallback;

final class Cli {
    private final static String CLI_VERSION = "libgenaro-java-1.0.0";

    private final static String  HELP_TEXT =
            "usage: genaro [<options>] <command> [<args>]\n\n" +
            "These are common Genaro commands for various situations:\n\n" +
            "working with buckets and files\n" +
            "  list-buckets\n" +
            "  list-files <bucket-id>\n" +
            "  remove-file <bucket-id> <file-id>\n" +
            "  remove-bucket <bucket-id>\n" +
            "  rename-bucket <bucket-id> <new-bucket-name>\n" +
            "  list-mirrors <bucket-id> <file-id>\n\n" +
            "downloading and uploading files\n" +
            "  upload-file <bucket-id> <path>\n" +
            "  download-file <bucket-id> <file-id> <path>\n" +
            "bridge genaro information\n" +
            "  get-info\n\n" +
            "options:\n" +
            "  -h, --help                output usage information\n" +
            "  -v, --version             output the version number\n" +
            "  -u, --url <url>           set the bridge host\n" +
            "  -w, --wallet <path>       set the path of wallet file\n" +
            "  -l, --log <level>         set the log level (default 0)\n" +
            "  -d, --debug               set the debug log level\n\n" +
            "environment variables:\n" +
            "  GENARO_BRIDGE             the bridge host\n" +
            "  GENARO_WALLET             the path of wallet file";

    public static void main(String[] args) {
        String genaroBridge = System.getenv("GENARO_BRIDGE");
        String genaroWallet = System.getenv("GENARO_WALLET");
        int c;
        int logLevel = 0;

        LongOpt [] longOpts = new LongOpt[6];
        longOpts[0] = new LongOpt("url", LongOpt.NO_ARGUMENT, null, 'u');
        longOpts[1] = new LongOpt("wallet", LongOpt.REQUIRED_ARGUMENT, null, 'w');
        longOpts[2] = new LongOpt("version", LongOpt.REQUIRED_ARGUMENT, null, 'v');
        longOpts[3] = new LongOpt("log", LongOpt.REQUIRED_ARGUMENT, null, 'l');
        longOpts[4] = new LongOpt("debug", LongOpt.NO_ARGUMENT, null, 'd');
        longOpts[5] = new LongOpt("help", LongOpt.REQUIRED_ARGUMENT, null, 'h');

        Getopt g = new Getopt("libgenaro-java", args, "u:w:l:dVvh", longOpts);

        while ((c = g.getopt()) != -1)
        {
            switch (c) {
                case 'u':
                    genaroBridge = g.getOptarg();
                    break;
                case 'w':
                    genaroWallet = g.getOptarg();
                    break;
                case 'l':
                    logLevel = Integer.parseInt(g.getOptarg());
                    break;
                case 'd':
                    logLevel = 4;
                    break;
                case 'V':
                case 'v':
                    System.out.println(CLI_VERSION + "\n");
                    System.exit(0);
                case 'h':
                    // fall through
                default:
                    System.out.println(HELP_TEXT);
                    System.exit(0);
            }
        }

        if (logLevel > 4 || logLevel < 0) {
            System.out.println("Invalid log level\n");
            System.exit(0);
        }

        if (args.length == 0) {
            System.out.println(HELP_TEXT);
            System.exit(0);
        }

        if (genaroBridge == null) {
            genaroBridge = "http://47.100.33.60:8080";
        }

        int commandIndex = g.getOptind();
        if (args.length <= commandIndex) {
            System.out.println(HELP_TEXT);
            System.exit(0);
        }
        String command = args[commandIndex];

        // Parse the host, part and proto from the genaro bridge url
        String proto;
        String host;
        int port;

        Pattern pattern = Pattern.compile("(.+):\\/\\/(.+):(\\d+)");
        Matcher matcher = pattern.matcher(genaroBridge);
        if (!matcher.find()) {
            System.out.println("Invalid bridge url\n");
            System.exit(0);
        }

        proto = matcher.group(1);
        host = matcher.group(2);
        port = Integer.parseInt(matcher.group(3));

        if (port == 0) {
            if (proto.equals("https")) {
                port = 443;
            } else {
                port = 80;
            }
        }

        genaroBridge = String.format("%s://%s:%d", proto, host, port);

        Genaro genaro = null;

        if (command.equals("get-info")) {
            genaro = new Genaro(genaroBridge);
            System.out.println(String.format("Genaro bridge: %s\n", genaroBridge));
            String info = genaro.getInfo();

            if (info == null) {
                System.out.println("Unable to get bridge information");
            } else {
                System.out.println(info);
            }

            System.exit(0);
        }

        if (genaroWallet == null) {
            System.out.println("Wallet file not set");
            System.exit(0);
        }

        if (!Files.exists(Paths.get(genaroWallet))) {
            System.out.println("Wallet file not found");
            System.exit(0);
        }

        // read wallet file
        StringBuilder sb = new StringBuilder();
        String line;
        try (FileReader reader = new FileReader(genaroWallet);
             BufferedReader br = new BufferedReader(reader)) {
            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
            }
        } catch (IOException e) {
            System.out.println("Wallet file read error");
            System.exit(0);
        }

        String privKey = sb.toString();

        Console console = System.console();
        if (console == null) {
            System.out.println("Couldn't get Console instance");
            System.exit(0);
        }
        char passwdChars[] = console.readPassword("Enter password: ");
        String passwd = new String(passwdChars);

        try {
            genaro = new Genaro(genaroBridge, privKey, passwd, logLevel);
        } catch (Exception e) {
            System.out.println("Wallet invalid or password incorrect");
            System.exit(0);
        }

        if (command.equals("download-file")) {
            if (args.length < commandIndex + 4) {
                System.out.println("Missing arguments, expected: <bucket-id> <file-id> <path>");
                System.exit(0);
            }

            String bucketId = args[commandIndex + 1];
            String fileId = args[commandIndex + 2];
            String path = args[commandIndex + 3];

            Downloader downloader = genaro.resolveFile(bucketId, fileId, path, true, new ResolveFileCallback() {
                @Override
                public void onBegin() {
                    System.out.println("Download started");
                }
                @Override
                public void onProgress(float progress) {
                    System.out.printf("Download progress: %.1f%%\n", progress * 100);
                }
                @Override
                public void onFail(String error) {
                    System.out.println("Download failed, reason: " + (error != null ? error : "Unknown"));
                }
                @Override
                public void onCancel() {
                    System.out.println("Download is cancelled");
                }
                @Override
                public void onFinish() {
                    System.out.println("Download finished");
                }
            });

            downloader.join();
            System.exit(0);
        } else if (command.equals("upload-file")) {
            if (args.length < commandIndex + 3) {
                System.out.println("Missing arguments, expected: <bucket-id> <path>");
                System.exit(0);
            }

            String bucketId = args[commandIndex + 1];
            String path = args[commandIndex + 2];
            File file = new File(path);
            String name = file.getName();

            Uploader uploader = genaro.storeFile(false, path, name, bucketId, new StoreFileCallback() {
                @Override
                public void onBegin(long fileSize) {
                    System.out.println("Upload started");
                }
                @Override
                public void onProgress(float progress) {
                    System.out.printf("Upload progress: %.1f%%\n", progress * 100);
                }
                @Override
                public void onFail(String error) {
                    System.out.println("Upload failed, reason: " + (error != null ? error : "Unknown"));
                }
                @Override
                public void onCancel() {
                    System.out.println("Upload is cancelled");
                }
                @Override
                public void onFinish(String fileId) {
                    System.out.println("Upload finished, fileId: " + fileId);
                }
            });

            uploader.join();
            System.exit(0);
        } else if (command.equals("list-files")) {
            if (args.length < commandIndex + 2) {
                System.out.println("Missing first argument: <bucket-id>");
                System.exit(0);
            }

            String bucketId = args[commandIndex + 1];

            CompletableFuture<Void> fu = genaro.listFiles(bucketId, new ListFilesCallback() {
                @Override
                public void onFinish(GenaroFile[] files) {
                    if(files.length == 0) {
                        System.out.println("No files.");
                    } else {
                        for (GenaroFile b : files) {
                            System.out.println(b.toBriefString());
                        }
                    }
                }
                @Override
                public void onFail(String error) {
                    System.out.println("List files failed, reason: " + error + ".");
                }
            });

            fu.join();
            System.exit(0);
        } else if (command.equals("remove-bucket")) {
            if (args.length < commandIndex + 2) {
                System.out.println("Missing first argument: <bucket-id>");
                System.exit(0);
            }

            String bucketId = args[commandIndex + 1];
            CompletableFuture<Void> fu = genaro.deleteBucket(bucketId, new DeleteBucketCallback() {
                @Override
                public void onFinish() {
                    System.out.println("Delete bucket success.");
                }

                @Override
                public void onFail(String error) {
                    System.out.println("Delete bucket failed, reason: " + error + ".");
                }
            });

            fu.join();
            System.exit(0);
        } else if (command.equals("remove-file")) {
            if (args.length < commandIndex + 3) {
                System.out.println("Missing arguments, expected: <bucket-id> <file-id>");
                System.exit(0);
            }

            String bucketId = args[commandIndex + 1];
            String fileId = args[commandIndex + 2];

            CompletableFuture<Void> fu = genaro.deleteFile(bucketId, fileId, new DeleteFileCallback() {
                @Override
                public void onFinish() {
                    System.out.println("Delete file success.");
                }
                @Override
                public void onFail(String error) {
                    System.out.println("Delete file failed, reason: " + error + ".");
                }
            });

            fu.join();
            System.exit(0);
        } else if (command.equals("list-buckets")) {
            CompletableFuture<Void> fu = genaro.getBuckets(new GetBucketsCallback() {
                @Override
                public void onFinish(Bucket[] buckets) {
                    if(buckets.length == 0) {
                        System.out.println("No buckets.");
                    } else {
                        for (Bucket b : buckets) {
                            System.out.println(b);
                        }
                    }
                }
                @Override
                public void onFail(String error) {
                    System.out.println("List buckets failed, reason: " + error + ".");
                }
            });

            fu.join();
            System.exit(0);
        } else if (command.equals("list-mirrors")) {
            if (args.length < commandIndex + 3) {
                System.out.println("Missing arguments, expected: <bucket-id> <file-id>");
                System.exit(0);
            }

            String bucketId = args[commandIndex + 1];
            String fileId = args[commandIndex + 2];

            CompletableFuture<Void> fu = genaro.listMirrors(bucketId, fileId, new ListMirrorsCallback() {
                @Override
                public void onFinish(String text) {
                    System.out.println(text);
                }
                @Override
                public void onFail(String error) {
                    System.out.println("List mirrors failed, reason: " + error + ".");
                }
            });

            fu.join();
            System.exit(0);
        } else if (command.equals("rename-bucket")) {
            if (args.length < commandIndex + 3) {
                System.out.println("Missing arguments, expected: <bucket-id> <new_bucket-name>");
                System.exit(0);
            }

            String bucketId = args[commandIndex + 1];
            String newBucketName = args[commandIndex + 2];

            CompletableFuture<Void> fu = genaro.renameBucket(bucketId, newBucketName, new RenameBucketCallback() {
                @Override
                public void onFinish() {
                    System.out.println("Rename bucket success.");
                }
                @Override
                public void onFail(String error) {
                    System.out.println("Rename bucket failed, reason: " + error + ".");
                }
            });

            fu.join();
            System.exit(0);
        } else {
            System.out.println(String.format("'%s' is not a genaro command. See 'genaro --help'\n", command));
            System.exit(0);
        }
    }
}
