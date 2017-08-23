package org.ipvp.bungeesync.event;

import net.md_5.bungee.api.connection.ProxiedPlayer;
import net.md_5.bungee.api.plugin.Event;

public class RecalculatePlayerEvent extends Event {

    private final ProxiedPlayer player;
    
    public RecalculatePlayerEvent(ProxiedPlayer player) {
        this.player = player;
    }

    public ProxiedPlayer getPlayer() {
        return player;
    }
}
