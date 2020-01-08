package actorMapreduce;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class MovieInfo implements WritableComparable<MovieInfo> {
    private String title;
    private  int year;
    private  String type;
    private  float star;
    private  String director;
    private  String actor;
    private  String time;
    private  String film_page;
    private int pp;
    private String _id;

    @Override
    public String toString(){
        return "[title="+title+",actor="+actor+",film_page="+film_page+",type="+type+",star="+star+",year="+year+"]";
    }
    @Override
    //按star从大到小排序，当star相同时按year从大到小排序
    public int compareTo (MovieInfo m) {
        if (this.star < m.getStar()) {
            return 1;
        } else if (this.star > m.getStar()) {
            return -1;
        }
        if (this.year <= m.getYear()) {
            return 1;
        } else {
            return -1;
        }
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.title=in.readUTF();
        this.year=in.readInt();
        this.type=in.readUTF();
        this.star=in.readFloat();
        this.director=in.readUTF();
        this.actor=in.readUTF();
        this.time=in.readUTF();
        this.film_page=in.readUTF();
        this.pp=in.readInt();
        this._id=in.readUTF();

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.title);
        out.writeInt(this.year);
        out.writeUTF(this.type);
        out.writeFloat(this.star);
        out.writeUTF(this.director);
        out.writeUTF(this.actor);
        out.writeUTF(this.time);
        out.writeUTF(this.film_page);
        out.writeInt(this.pp);
        out.writeUTF(this._id);

    }



    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public float getStar() {
        return star;
    }

    public void setStar(float star) {
        this.star = star;
    }

    public String getDirector() {
        return director;
    }

    public void setDirector(String director) {
        this.director = director;
    }

    public String getActor() {
        return actor;
    }

    public void setActor(String actor) {
        this.actor = actor;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getFilm_page() {
        return film_page;
    }

    public void setFilm_page(String film_page) {
        this.film_page = film_page;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public int getPp() {
        return pp;
    }

    public void setPp(int pp) {
        this.pp = pp;
    }
}
