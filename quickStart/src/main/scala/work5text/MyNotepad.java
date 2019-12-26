package work5text;

import java.awt.*;
import java.awt.event.*;
import java.io.*;
import javax.swing.*;

public class MyNotepad implements ActionListener
{
    private JFrame frame=new JFrame("新记事本");
    private JTextArea jta=new JTextArea();
    private String result="";
    private boolean flag=true;
    private File f;
    private JButton jb=new JButton("开始");
    private JTextField jtf=new JTextField(15);
    private JTextField jt=new JTextField(15);
    private JButton jbt=new JButton("替换为");
    private JButton jba=new JButton("全部替换");
    private Icon ic=new ImageIcon("D:\\java课堂笔记\\GUI\\11.gif");
    private String value;
    private int start=0;
    private JFrame jf=new JFrame("查找");
    private JFrame jfc=new JFrame("替换");


    /**
     * 事件监听方法 ，重写actionPerformed
     * @param e
     */
    @Override
    public void actionPerformed(ActionEvent e)
    {
        String comm=e.getActionCommand();
        if("新建".equals(comm))
        {
            if(!(frame.getTitle().equals("新记事本")))
            {
                if(!flag)
                {
                    write();
                    newNew();
                }
                else
                {
                    JFileChooser jfc=new JFileChooser("D:\\java课堂笔记");
                    int returnVal = jfc.showDialog(null,"保存为");
                    if(returnVal == JFileChooser.APPROVE_OPTION)
                    {//选择文件后再执行下面的语句，保证了程序的健壮性
                        f=jfc.getSelectedFile();
                        flag=false;
                        write();
                    }
                }
            }
            else if(!(jta.getText().isEmpty()))
            {
                JFileChooser jfc=new JFileChooser("D:\\java课堂笔记");
                int returnVal = jfc.showDialog(null,"保存为");
                if(returnVal == JFileChooser.APPROVE_OPTION)
                {//选择文件后再执行下面的语句，保证了程序的健壮性
                    f=jfc.getSelectedFile();
                    flag=false;
                    write();
                    newNew();
                }
            }
            else
            {
                newNew();
            }
        }
        else if("打开".equals(comm))
        {
            JFileChooser jfc=new JFileChooser("D:\\java课堂笔记");
            jfc.setDialogType(JFileChooser.OPEN_DIALOG);
            int returnVal = jfc.showOpenDialog(null);
            if(returnVal == JFileChooser.APPROVE_OPTION)
            {//选择文件后再执行下面的语句，保证了程序的健壮性
                f=jfc.getSelectedFile();
                frame.setTitle(f.getName());
                result=read();
                flag=false;
                value=result;
                jta.setText(result);
            }
        }else if("保存".equals(comm))
        {
            JFileChooser jfc=new JFileChooser("D:\\java课堂笔记");
            if(flag)
            {
                int returnVal = jfc.showDialog(null,"保存为");
                if(returnVal == JFileChooser.APPROVE_OPTION)
                {//选择文件后再执行下面的语句，保证了程序的健壮性
                    f=jfc.getSelectedFile();
                    flag=false;
                    write();
                }
            }
            else
            {
                write();
            }
        }
        else if("另存".equals(comm))
        {
            JFileChooser jfc=new JFileChooser("D:\\java课堂笔记");
            int returnVal = jfc.showDialog(null,"另存");
            if(returnVal == JFileChooser.APPROVE_OPTION)
            {//选择文件后再执行下面的语句，保证了程序的健壮性
                f=jfc.getSelectedFile();
                write();
            }
        }
        else if("退出".equals(comm))
        {
            System.exit(0);
        }
        else if("撤销".equals(comm))
        {
            jta.setText(value);
        }
        else if("剪切".equals(comm))
        {
            value=jta.getText();
            jta.cut();
        }
        else if("复制".equals(comm))
        {
            jta.copy();
        }
        else if("粘贴".equals(comm))
        {
            value=jta.getText();
            jta.paste();
        }
        else if("删除".equals(comm))
        {
            value=jta.getText();
            jta.replaceSelection(null);
        }
        else if("全选".equals(comm))
        {
            jta.selectAll();
        }
        else if("查找".equals(comm))
        {
            value=jta.getText();
            jf.add(jtf,BorderLayout.CENTER);
            jf.add(jb,BorderLayout.SOUTH);
            jf.setLocation(300,300);
            jf.pack();
            jf.setVisible(true);
            jf.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        }
        else if("替换".equals(comm))
        {
            value=jta.getText();
            GridLayout gl=new GridLayout(3,3);
            JLabel jl1=new JLabel("查找内容:");
            JLabel jl2=new JLabel("替换为:");
            jfc.setLayout(gl);
            jfc.add(jl1);
            jfc.add(jtf);
            jfc.add(jb);
            jfc.add(jl2);
            jfc.add(jt);
            jfc.add(jbt);
            JLabel jl3=new JLabel();
            JLabel jl4=new JLabel();
            jfc.add(jl3);
            jfc.add(jl4);
            jfc.add(jba);
            jfc.setLocation(300,300);
            jfc.pack();
            jfc.setVisible(true);
            jfc.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        }
        else if("版本".equals(comm))
        {
            JDialog jd=new JDialog(frame,"关于对话框");
            jd.setSize(200,200);
            JLabel l=new JLabel("TEXT PAD \n version-1.0");
            jd.add(l,BorderLayout.CENTER);
            jd.setLocation(100,200);
            jd.setSize(300,300);
            jd.setVisible(true);
            //   jd.pack();
            jd.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
        }
        else if("开始".equals(comm)||"下一个".equals(comm))
        {
            String temp=jtf.getText();
            int s=value.indexOf(temp,start);
            if(value.indexOf(temp,start)!=-1)
            {
                jta.setSelectionStart(s);
                jta.setSelectionEnd(s+temp.length());
                jta.setSelectedTextColor(Color.GREEN);
                start=s+1;
                jb.setText("下一个");
                //   value=value.substring(s+temp.length());//不能截取字串
            }
            else
            {
                JOptionPane.showMessageDialog(jf, "查找完毕!", "提示", 0, ic);
                jf.dispose();
            }
        }
        else if("替换为".equals(comm))
        {
            String temp=jtf.getText();
            int s=value.indexOf(temp,start);
            if(value.indexOf(temp,start)!=-1)
            {
                jta.setSelectionStart(s);
                jta.setSelectionEnd(s+temp.length());
                jta.setSelectedTextColor(Color.GREEN);
                start=s+1;
                jta.replaceSelection(jt.getText());
            }
            else
            {
                JOptionPane.showMessageDialog(jf, "查找完毕!", "提示", 0, ic);
                jf.dispose();
            }
        }
        else if("全部替换".equals(comm))
        {
            String temp=jta.getText();
            temp=temp.replaceAll(jtf.getText(), jt.getText());
            jta.setText(temp);

        }
    }


    /**
     * 读方法
     * @return
     */
    public String read()
    {
        String temp="";
        try
        {
            FileInputStream fis = new FileInputStream(f.getAbsolutePath());
            byte[] b=new byte[1024];
            while(true)
            {
                int num=fis.read(b);
                if(num==-1)
                    break;
                temp=temp+new String(b,0,num);
            }
            fis.close();
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }
        return temp;
    }


    /**
     * 写方法
     */
    public void write()
    {
        try
        {
            FileOutputStream fos=new FileOutputStream(f);
            fos.write(jta.getText().getBytes());
            fos.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    /**
     * 新建文本
     */
    public void newNew()
    {
        frame.dispose();
        new MyNotepad();
        flag=true;
    }

    /**
     * 主界面方法
     */
    public MyNotepad()
    {
        JMenuBar jmb=new JMenuBar();
        String[] menuLab={"文件","编辑","帮助"};
        String[][] menuItemLab={{"新建","打开","保存","另存","退出"},
                {"撤销","剪切","复制","粘贴","删除","全选","查找","替换"},
                {"版本"}};
        for(int i=0;i<menuLab.length;i++)
        {
            JMenu menu=new JMenu(menuLab[i]);
            jmb.add(menu);
            for(int j=0;j<menuItemLab[i].length;j++)
            {
                JMenuItem jmi=new JMenuItem(menuItemLab[i][j]);
                menu.add(jmi);
                jmi.addActionListener(this);
            }
        }
        frame.setJMenuBar(jmb);
        jta.setLineWrap(true);//自动换行
        JScrollPane jsp=new JScrollPane(jta);//滚动窗口面板
        frame.add(jsp);

        jb.addActionListener(this);
        jbt.addActionListener(this);
        jba.addActionListener(this);

        frame.setLocation(200,50);
        frame.setSize(620,660);
        frame.setVisible(true);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }


    /**
     *  入口程序
     * @param args
     */
    public static void main(String[] args)
    {
        new MyNotepad();
    }
}