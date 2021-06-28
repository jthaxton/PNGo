package photoProcessor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import javax.imageio.ImageIO;
import javax.swing.JFrame;

public class PhotoProcessor {
    BufferedImage  image;
    int width;
    int height;

    public PhotoProcessor(ConsumerRecord<String, String> record) {
        String message = record.value();
        try {
            URL input = new URL(message);
            image = ImageIO.read(input);
            width = image.getWidth();
            height = image.getHeight();

            for(int i=0; i<height; i++) {

                for(int j=0; j<width; j++) {

                    Color c = new Color(image.getRGB(j, i));
                    int red = (int)(c.getRed() * 0.299);
                    int green = (int)(c.getGreen() * 0.587);
                    int blue = (int)(c.getBlue() *0.114);
                    Color newColor = new Color(red+green+blue,

                            red+green+blue,red+green+blue);

                    image.setRGB(j,i,newColor.getRGB());
                }
            }

            File ouptut = new File("grayscale.jpg");
            ImageIO.write(image, "jpg", ouptut);

        } catch (Exception e) {}
    }
}
