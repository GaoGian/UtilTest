import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by tingyun on 2018/8/1
 * 解析svn提交日志，获取用户信息，生成svn-git用户对照信息
 */
public class SvnLogTest {

    public static void main(String[] args){
        Set<String> userNames = new HashSet<>();

        String readFilePath = "C:\\Users\\tingyun\\Desktop\\临时\\svn_log.txt";
        String writeFilePath = "C:\\Users\\tingyun\\Desktop\\临时\\svn_user_list.txt";

        FileReader reader = null;
        try {
            reader = new FileReader(readFilePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        FileWriter writer = null;
        try {
            writer = new FileWriter(writeFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader br = new BufferedReader(reader);
        try {
            String line = null;
            while ((line = br.readLine()) != null) {
                try {
                    if (line.startsWith("r") && line.contains("|")) {
                        String userName = line.split("\\|")[1].trim();
                        userNames.add(userName);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for(String userName : userNames){
            try {
                writeFile(userName + " = " + userName + " <" + userName + "@tingyun.com>", writer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if(writer != null){
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private static void writeFile(String value, FileWriter writer) throws IOException {
        writer.write(value + "\r\n");
    }

}
