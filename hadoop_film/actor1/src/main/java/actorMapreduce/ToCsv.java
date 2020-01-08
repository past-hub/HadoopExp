package actorMapreduce;
import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import  java.io.FileWriter;

public class ToCsv {
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("./Film.json"));
        FileWriter sw = new FileWriter(new File("./Film.csv"));
        MovieInfo m = null;
        String line = br.toString();
        while ((line = br.readLine()) != null) {
            //Fastjson 把每行的json 字符串转换为对象。
            m = JSON.parseObject(line, MovieInfo.class);
            if (m.getActor().indexOf("约翰·赫特") != -1) {
                //Film_page 作为电影ID
                String mid=m.getFilm_page();
                //将title中的英文逗号替换为无
                String title = m.getTitle().replaceAll(",","");
                //取出演员的列表
                String[] actors=m.getActor().split(",");
                for(String ac:actors){
                    if (ac.indexOf("约翰·赫特")!=-1){
                        continue;
                    }else {
                    //把电影数据写入csv文件。csv 表头为 ID,电影名称,演员，评分，年份
                    sw.append(mid+","+title+","+ac+","+m.getStar()+","+m.getYear()+"\n");
                    }
                }
            }
        }
        sw.close();
        br.close();
    }
}