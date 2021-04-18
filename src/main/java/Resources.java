import java.io.*;
import java.net.*;
import java.nio.file.*;

public class Resources{

    public static String getResourcePath() throws URISyntaxException, IOException {
        
        URI resourcePathFile = Resources.class.getResource("/RESOURCE_PATH").toURI();
        String resourcePath = Files.readAllLines(Paths.get(resourcePathFile)).get(0);
        return resourcePath;
    }
}