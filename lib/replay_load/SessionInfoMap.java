import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
public class SessionInfoMap {
    private static Map<String,String> m = Collections.synchronizedMap(new HashMap<String,String>());
    
    public static void put(String k, String v) throws Exception{
        m.put(k,v);
    }
    public static Object get(String k) throws Exception{
        return m.get(k);
    }
    public static boolean containsKey(String k) throws Exception{
        return m.containsKey(k);
    }	
    public static void clear() throws Exception{
        m.clear();
    }

    public static void main(String [] args) throws Exception{
    	SessionInfoMap.put("test-original-hash-1","newjession1");
        SessionInfoMap.put("test-original-hash-2","newjsession2");
        //q.clear();
        System.out.println(SessionInfoMap.get("test-original-hash-1"));
	System.out.println(SessionInfoMap.get("test-original-hash-2"));
    }
}
