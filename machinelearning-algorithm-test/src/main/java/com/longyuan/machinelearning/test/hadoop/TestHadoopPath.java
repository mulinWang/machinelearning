package com.longyuan.machinelearning.test.hadoop;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by mulin on 2017/11/24.
 */
public class TestHadoopPath {
    private URI uri;

    @Test
    public void mapTest() {
        String pathString = "exit_00:08:00_2.log";
        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        int colon = pathString.indexOf(':');
        int slash = pathString.indexOf('/');

        if (pathString.startsWith("") )

        if ((colon != -1) &&
                ((slash == -1) || (colon < slash))) {     // has a scheme
            scheme = pathString.substring(0, colon);
            start = colon+1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start) &&
                (pathString.length()-start > 2)) {       // has authority
            int nextSlash = pathString.indexOf('/', start+2);
            int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start+2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        String path = pathString.substring(start, pathString.length());

        initialize(scheme, authority, path, null);
    }

    private void initialize(String scheme, String authority, String path,
                            String fragment) {
        try {
            this.uri = new URI(scheme, authority, normalizePath(scheme, path), null, fragment)
                    .normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static String normalizePath(String scheme, String path) {
        // Remove double forward slashes.
        path = StringUtils.replace(path, "//", "/");

        // Remove backslashes if this looks like a Windows path. Avoid
        // the substitution if it looks like a non-local URI.

        // trim trailing slash from non-root path (ignoring windows drive)
        int minLength = startPositionWithoutWindowsDrive(path) + 1;
        if (path.length() > minLength && path.endsWith("/")) {
            path = path.substring(0, path.length()-1);
        }

        return path;
    }

    private static int startPositionWithoutWindowsDrive(String path) {
        return 0;
    }

}
