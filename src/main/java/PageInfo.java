import java.io.Serializable;
import java.util.Vector;

// class to hold needed info for each page
class PageInfo implements Serializable{
    
    // to hold the reference "name" of the page on the desk
    private String pageName;
    // min and max value of the promarykey in this tabel
    Object min, max;
    
    public PageInfo(){
        // each page would have a unique name which is the hash of the object points to it
        this.pageName = this.toString();
        System.out.println("this is a Page");
    }

    public String getPageName(){
        return this.pageName;
    }
}