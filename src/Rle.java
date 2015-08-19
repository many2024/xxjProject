import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.swing.JOptionPane;

public class Rle {

	public  void compress(String path) throws Exception {
		
        String path1=path+".rle";
		byte[] data=readFile(path);
		
		
		
		compessRLE(data,path1);
		JOptionPane.showMessageDialog(null,"���ѹ��");
		
		
	}
	
	public  void compessRLE(byte[] data,String fileout) throws Exception{
//		String fileout="e:" + File.separator + "out.rle";
		OutputStream out = null ;	// ׼����һ������Ķ���
		out = new FileOutputStream(fileout)  ;	// ͨ������̬�ԣ�����ʵ��
		java.io.DataOutputStream dos = new java.io.DataOutputStream(out);
		int dataLength=data.length;
//		System.out.println("����δ"+dataLength);
		int count=1;
//		int fing=0;
		for(int i=0;i<dataLength;i++){        //����������ж����ֽ�
			System.out.println("ѹ���У������Ժ�");
			int icount=1;
//			fing++;
			for(int j=i+1;j<dataLength;j++){  //�жϺ�����Ƿ��������ֽ���ͬ
				
				
				if(data[i]==data[j]){
					count++;
					icount=count;
					if(i+icount==dataLength){
//						out.write(count);
						dos.writeInt(count);
						dos.write(data[i]);
						
//						System.out.println(count+";"+(char)data[i]);
//						dataString=dataString+count+data[i];
						
					}
				}else{
//					out.write(count);
					dos.writeInt(count);
					dos.write(data[i]);
//					System.out.println(count+";"+(char)data[i]);
//					dataString=dataString+count+data[i];
					 count=1;
					 
					break;
				}
			}
			
			i=i+icount-1;
//			System.out.println(i);
		}
	}
	
	public  byte[] readFile(String path) throws Exception{
		File f= new File(path) ;	
		InputStream input = null ;	
		input = new FileInputStream(f)  ;	
		int size=input.available(); 
		byte b[] = new byte[size] ;	
		
		input.read(b) ;	
		return b;
	}
	
	
	
	
}