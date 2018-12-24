package net.qihoo.xitong.xdml.utils;

public class ExitCodeResolver {

    static void analysis(int exitCode){
        switch(exitCode){
            case -1: {

            }break;
            case -2: {

            }break;
            case -3: {

            }break;
            case -4: {

            }break;
            case -5: {

            }break;
            case -6: {

            }break;
            case -7: {

            }break;
            default:
                throw new IllegalArgumentException("Unknown exit code.");
        }
    }

}
