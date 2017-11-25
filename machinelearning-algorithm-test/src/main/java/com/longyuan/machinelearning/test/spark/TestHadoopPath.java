package com.longyuan.machinelearning.test.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.regex.Pattern;

/**
 * Created by XingBaoBao on 2017/11/24.
 */
public class TestHadoopPath {

    public static final String csdataS3Filepattern = "^(\\w*)_(\\d{2}):(\\d{2}):(\\d{2})_(\\d*).log";

    @Test
    public void testPath()  {
        String pathString = "exit_01:02:48_2.log";
        boolean isMatch = Pattern.matches(csdataS3Filepattern, pathString);

        if (isMatch) {
//            pathString = pathString.replaceAll(":", "%3A");
//
//            URI uri =  new URI(pathString);
            try {
                pathString =  URLEncoder.encode(pathString, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        System.out.printf("content:" + pathString);
        assert isMatch == true;

    }

    public static final String test = "(\\w*)_(\\d{2}):(\\d{2}):(\\d{2})_(\\d*).log";

    @Test
    public void testPattern() {
        String content = "aa_01:00:21_2121.log";
        boolean isMatch = Pattern.matches(test, content);

        if (isMatch) {
            content = content.replaceAll(":", "_");
        }
        System.out.printf("content:" + content);
        assert isMatch == true;

    }

    URI uri;

    @Test
    public void test() {
        String pathString = "exit_01:02:48_2.log";
        // parse uri components
        String scheme = null;
        String authority = null;

        boolean isMatch = Pattern.matches(csdataS3Filepattern, pathString);

        String path = pathString;
        if (!isMatch) {
            int start = 0;

            // parse uri scheme, if any
            int colon = pathString.indexOf(':');
            int slash = pathString.indexOf('/');
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
             path = pathString.substring(start, pathString.length());
        }

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
