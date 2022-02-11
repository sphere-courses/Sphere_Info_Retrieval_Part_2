import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

class LinksExtractor {
    private Matcher pageMatcher;
    private URI baseURI;
    private String link;

    LinksExtractor(String text, String baseDomain){
        this.baseURI = URI.create(baseDomain);

        byte[] decoded = Base64.getDecoder().decode(text);

        Inflater inflater = new Inflater();
        inflater.setInput(decoded);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte outBuffer[] = new byte[1024];
        while (!inflater.finished()) {
            int bytesInflated;
            try {
                bytesInflated = inflater.inflate(outBuffer);
            } catch (DataFormatException e) {
                throw new RuntimeException(e.getMessage());
            }

            baos.write(outBuffer, 0, bytesInflated);
        }
        String HTMLPage = "";
        try {
            HTMLPage = baos.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        final String linkRegex = "<a[^>]+href=[\"']?([^\"'\\s>]+)[\"']?[^>]*>";
        Pattern linkPattern = Pattern.compile(linkRegex,Pattern.CASE_INSENSITIVE|Pattern.DOTALL);
        pageMatcher = linkPattern.matcher(HTMLPage);
    }

    boolean finished() {
        while (pageMatcher.find()){
            link = pageMatcher.group(1);
            URI uri;
            try {
                uri = URI.create(link);
            } catch (IllegalArgumentException ex){
                continue;
            }
            if(uri.getHost() == null && uri.getPath() == null){
                continue;
            }
            else if(uri.getHost() == null) {
                if(uri.getPath().length() == 0){
                    continue;
                }
                try {
                    uri = baseURI.resolve(uri.getPath().substring(1));
                } catch (IllegalArgumentException ex){
                    continue;
                }
            } else if(!uri.getHost().contains("lenta.ru")){
                continue;
            }
            String URIHost = uri.getHost();
            String URIPath = uri.getPath();
            if(URIHost != null){
                URIHost = URIHost.replace("\"", "");
            }
            if(URIPath != null){
                URIPath = URIPath.replace("\"", "");
            }
            link = "http://" + URIHost + URIPath;
            return false;
        }
        return true;
    }

    String getNextLink(){
        return link;
    }

}
