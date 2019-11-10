package hadoop.ch05.v17124080229;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
public class sogou implements Writable {
    //00:00:06 9441949621471399   [韩国饰品] 1 1    www.uczx.com/
    //时间 用户id 查询词 排名 顺序 url
    //由以上定义变量
    private String date;
    private String userid;
    private String word;
    //  private String c;
    private int rank;
    private int order;
    private String url;

    @Override
    public String toString() {
        return "Data{" +
                "date='" + date + '\'' +
                ", userid='" + userid + '\'' +
                ", word='" + word + '\'' +
                ", rank=" + rank +
                ", order=" + order +
                ", url='" + url + '\'' +
                '}';
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.date);
        out.writeUTF(this.userid);
        out.writeUTF(this.word);
        out.writeInt(this.rank);
        out.writeInt(this.order);
        out.writeUTF(this.url);

    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = in.readUTF();
        this.userid = in.readUTF();
        this.word = in.readUTF();
        this.rank = in.readInt();
        this.order = in.readInt();
        this.url = in.readUTF();
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
