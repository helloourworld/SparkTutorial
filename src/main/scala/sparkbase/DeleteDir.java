package main.scala.sparkbase;

import java.io.File;

/**
 * Created by hadoop on 2016/11/16.
 */
public class DeleteDir {

    public static boolean delete(File dir)
    {
        if (dir.isDirectory())
        {
            File[] listFiles = dir.listFiles();
            for (int i = 0; i < listFiles.length && delete(listFiles[i]); i++)
            {
            }
        }
        return dir.delete();
    }
}
