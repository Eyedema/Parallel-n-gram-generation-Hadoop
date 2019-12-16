import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class HDFSFileReader {

    private ArrayList<String> books = new ArrayList<String>();
    private String path;
    private String NAME_NODE;

    public HDFSFileReader(String NAME_NODE, String path) {
        this.NAME_NODE = NAME_NODE;
        this.path = path;
    }

    public ArrayList<String> readBooks() throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new URI(NAME_NODE), new Configuration());
        Path booksPath = new Path(path);
        for (int i = 1; i < 10; i++) {
            FSDataInputStream fileContent = fs.open(new Path(path + "/" + i + ".txt"));
            byte[] bs = new byte[fileContent.available()];
            fileContent.readFully(bs);
            books.add(new String(bs));
        }
        return books;
    }

}
