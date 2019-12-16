import java.io.IOException;
import java.net.URISyntaxException;

public class main {

    private static final String NAME_NODE = "hdfs://localhost:9000";

    public static void main(String[] args) throws URISyntaxException, IOException {
        System.setProperty("hadoop.home.dir", "/home/eyedema/hadoop");
        HDFSFileReader hdfsfr = new HDFSFileReader(NAME_NODE, args[0]);
        hdfsfr.readBooks();
    }

}