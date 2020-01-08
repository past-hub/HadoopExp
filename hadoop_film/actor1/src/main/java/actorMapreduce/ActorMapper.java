package actorMapreduce;
import java.io.IOException;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ActorMapper extends Mapper<LongWritable, Text, MovieInfo, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line=value.toString();//将json对象转换为json字符串
        MovieInfo movi = JSON.parseObject(line,MovieInfo.class);//将json字符串转换为Java对象
        //添加限制条件
        if (movi.getActor().indexOf("约翰·赫特") != -1) {
// 数据格式：{"_id":{"$oid":"5ad89b4b6afaf881d81e6d0b"},"title":"红颜孽 Nasha","year":"2013","type":"剧情,喜剧,动作,爱情,音乐,歌舞,家庭","star":3.9,"director":"Amit Saxena","actor":"Ranbir Chakma,Nikhil Desai,Raj Kesaria,Rohan Khurana,Chirag Lobo,帕纳姆.潘迪,Shivam Patil,Sheetal Singh,Mohit Chauhan","pp":86,"time":122,"film_page":"https://movie.douban.com/subject/25721849/"}
            context.write(movi,NullWritable.get());
        }

    }
}
