package ordo;
import map.Mapper;
import map.Reducer;
import formats.Format;

public class WorkerImpl implements Worker {
    
    public WorkerImpl() {
    }

    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) {
        m.map(reader, writer);
        
        //map effectué, on ferme reader et writer
        reader.close();
        writer.close();

        //Sytem.out.println("Map local terminé");

        //Envoie du callBack cb
        //cb.???
    }
}