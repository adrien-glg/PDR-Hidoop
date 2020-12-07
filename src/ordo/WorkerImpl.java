package ordo;
import map.Mapper;
import map.Reducer;

import java.rmi.*;

import formats.Format;

public class WorkerImpl implements Worker {
    
    public WorkerImpl() {
    }
    
    public class doMap implements Runnable {
    	
    	Mapper m;
        Format reader;
        Format writer;
        CallBack cb;
        
		@Override
		public void run() {
			this.reader.open(Format.OpenMode.R);
            this.writer.open(Format.OpenMode.W);
            this.m.map(this.reader, this.writer);
            
            //Envoyer le callback qui previent du map fini
            
		}
    	
    }
    public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
        m.map(reader, writer);
        
        //map effectué, on ferme reader et writer
        reader.close();
        writer.close();

        //Sytem.out.println("Map local terminé");

        //Envoie du callBack cb
        //cb.???
    }
}