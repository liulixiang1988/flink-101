package source;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Access {
    private long time;
    private String domain;
    private double traffic;
}
