import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

class Utils {
    static double readDouble(Configuration configuration, String filePath) throws IOException {
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        return Double.valueOf(fs.open(path).readUTF());
    }

    static void writeDouble(Configuration configuration, String filePath, double value) throws IOException {
        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(configuration);
        fs.create(path).writeUTF(String.valueOf(value));
    }
}
