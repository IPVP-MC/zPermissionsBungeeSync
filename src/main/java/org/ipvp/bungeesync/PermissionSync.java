package org.ipvp.bungeesync;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;

import net.md_5.bungee.api.chat.TextComponent;
import net.md_5.bungee.api.connection.ProxiedPlayer;
import net.md_5.bungee.api.event.*;
import net.md_5.bungee.api.plugin.Listener;
import net.md_5.bungee.api.plugin.Plugin;
import net.md_5.bungee.config.Configuration;
import net.md_5.bungee.config.ConfigurationProvider;
import net.md_5.bungee.config.YamlConfiguration;
import net.md_5.bungee.event.EventHandler;
import net.md_5.bungee.event.EventPriority;
import net.md_5.bungee.util.CaseInsensitiveSet;

import org.ipvp.bungeesync.event.RecalculatePlayerEvent;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Created by Matthew on 21/02/2015.
 */
public class PermissionSync extends Plugin implements Listener {

    private HikariDataSource database;
    private Map<String, Group> cachedGroups;
    private Map<String, Consumer<ByteArrayDataInput>> bungeeMessageHandlers = new HashMap<>();
    private Multimap<ProxiedPlayer, Group> playerGroups = HashMultimap.create();
    private Map<UUID, Collection<Permission>> preLoadedPermissions = new ConcurrentHashMap<>();

    private final Consumer<ProxiedPlayer> playerSyncConsumer = p -> {
        try {
            recalculatePlayerPermissions(p);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    };
    private final Consumer<ByteArrayDataInput> playerHandler = in -> {
        UUID player = UUID.fromString(in.readUTF());
        ProxiedPlayer proxiedPlayer = getProxy().getPlayer(player);
        if (proxiedPlayer != null) {
            getProxy().getScheduler().schedule(this, () -> playerSyncConsumer.accept(proxiedPlayer),
                    1, TimeUnit.SECONDS);
        }
    };
    private final Consumer<Group> groupHandler = g -> {
        // Update all player permissions in the group
        getProxy().getScheduler().schedule(this, () -> refreshGroupMembers(g),1, TimeUnit.SECONDS);
    };
    
    private void refreshGroupMembers(Group group) {
        group.getPlayers().stream().map(getProxy()::getPlayer).forEach(playerSyncConsumer);
        group.getChildren().forEach(this::refreshGroupMembers);
    }

    @Override
    public void onEnable() {
        Configuration config;
        try {
            config = loadConfiguration();
            database = loadHikariDataSource(config);
        } catch (IOException | SQLException e) {
            getLogger().log(Level.SEVERE, "Failed to initiate config.yml or HikariCP connection - " +
                    "the plugin has been halted enabling!", e);
            return;
        }

        try {
            updateGroupPermissionCache();
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "Failed to update group permission cache", e);
            database.close();
            return;
        }
        
        // Register all player handlers
        bungeeMessageHandlers.put("PlayerDelete", in -> {
            handlePlayerAction(UUID.fromString(in.readUTF()), p -> {
                p.getPermissions().forEach(perm -> p.setPermission(perm, false)); // TODO: CME
                Collection<Group> groups = playerGroups.removeAll(p);
                groups.forEach(g -> g.removePlayer(p.getUniqueId()));
            });
        });
        bungeeMessageHandlers.put("PlayerSet", in -> {
            UUID player = UUID.fromString(in.readUTF());
            String permission = in.readUTF();
            handlePlayerAction(player, p -> p.setPermission(permission, true));
        });
        bungeeMessageHandlers.put("PlayerUnset", in -> {
            UUID player = UUID.fromString(in.readUTF());
            String permission = in.readUTF();
            handlePlayerAction(player, p -> p.setPermission(permission, false));
        });
        bungeeMessageHandlers.put("PlayerSetGroup", playerHandler);
        bungeeMessageHandlers.put("PlayerAddGroup", playerHandler);
        bungeeMessageHandlers.put("PlayerRemoveGroup", playerHandler);
        
        // Register all group handlers
        // TODO: Handling for group deletion
        bungeeMessageHandlers.put("GroupDelete", in -> {
            String group = in.readUTF();
            handleGroupAction(group, groupHandler);
            cachedGroups.remove(group);
        });
        bungeeMessageHandlers.put("GroupSet", in -> handleGroupAction(in.readUTF(), g -> {
            String permission = in.readUTF();
            g.addPermission(new Permission(permission, true));
            groupHandler.accept(g);
        }));
        bungeeMessageHandlers.put("GroupUnset", in -> handleGroupAction(in.readUTF(), g -> {
            String permission = in.readUTF();
            g.removePermission(new Permission(permission, true));
            groupHandler.accept(g);
        }));
        bungeeMessageHandlers.put("GroupCreate", in -> {
            String group = in.readUTF();
            cachedGroups.put(group, new Group(-1, group, 0));
        });
        // TODO: Properly implement GroupDeleteMembers
        bungeeMessageHandlers.put("GroupDeleteMembers", in -> handleGroupAction(in.readUTF(), groupHandler));

        getProxy().getPluginManager().registerListener(this, this);
        getProxy().getPlayers().forEach(playerSyncConsumer);
    }

    private void handlePlayerAction(UUID uuid, Consumer<ProxiedPlayer> onlinePlayerConsumer) {
        ProxiedPlayer player = getProxy().getPlayer(uuid);
        if (player != null) {
            onlinePlayerConsumer.accept(player);
        }
    }

    private void handleGroupAction(String group, Consumer<Group> groupConsumer) {
        Group g = cachedGroups.get(group);
        if (g != null) {
            groupConsumer.accept(g);
        }
    }

    /* (non-Javadoc)
     * Loads the configuration file
     */
    private Configuration loadConfiguration() throws IOException {
        File file = new File(getDataFolder(), "config.yml");

        if (file.exists()) {
            return ConfigurationProvider.getProvider(YamlConfiguration.class).load(file);
        }

        // Create the file to save
        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        file.createNewFile();

        // Load the default provided configuration and save it to the file
        Configuration config = ConfigurationProvider.getProvider(YamlConfiguration.class)
                .load(getResourceAsStream("config.yml"));
        ConfigurationProvider.getProvider(YamlConfiguration.class).save(config, file);
        return config;
    }

    /* (non-Javadoc)
     * Creates the HikariCP connection pool instance from configuration
     */
    private HikariDataSource loadHikariDataSource(Configuration config) throws SQLException {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(config.getString("database.url"));
        dataSource.setUsername(config.getString("database.user"));
        dataSource.setPassword(config.getString("database.pass"));
        dataSource.setMaximumPoolSize(config.getInt("database.threads"));
        dataSource.setThreadFactory(new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("hikari-sql-pool-%d").build());
        return dataSource;
    }

    @Override
    public void onDisable() {
        if (database != null) {
            database.close();
        }
    }

    public void updateGroupPermissionCache() throws SQLException {
        try (Connection connection = database.getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "SELECT entities.id id, name, priority, permission, value " +
                             "FROM entities " +
                             "JOIN entries " +
                             "ON entities.id = entries.entity_id " +
                             "WHERE entities.is_group = 1");
             ResultSet rs = ps.executeQuery()) {

            Map<String, Group> cachedGroups = new ConcurrentHashMap<>();
            while (rs.next()) {
                String name = rs.getString("name");
                Group group;
                if (cachedGroups.containsKey(name)) {
                    group = cachedGroups.get(name);
                } else {
                    group = new Group(rs.getInt("id"), name, rs.getInt("priority"));
                    cachedGroups.put(name, group);
                }

                Permission permission = new Permission(rs.getString("permission"), rs.getBoolean("value"));
                group.addPermission(permission);
            }
            this.cachedGroups = cachedGroups;
            updateGroupParents(connection);
        }
    }

    private void updateGroupParents(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT e1.name child_name, e2.name parent_name " +
                        "FROM inheritances " +
                        "JOIN entities e1 " +
                        "ON child_id = e1.id " +
                        "JOIN entities e2 " +
                        "ON inheritances.parent_id = e2.id");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Group child = cachedGroups.get(rs.getString("child_name"));
                Group parent = cachedGroups.get(rs.getString("parent_name"));
                child.setParent(parent);
            }
        }
    }

    /**
     * Recalculates the permissions of a player
     *
     * @param player Player to recalculate
     * @throws SQLException If getting permissions from database fails
     */
    public void recalculatePlayerPermissions(ProxiedPlayer player) throws SQLException {
        Collection<Permission> permissions = getPlayerPermissions(player.getUniqueId());
        updatePlayerPermissions(player, permissions);
    }

    private void updatePlayerPermissions(ProxiedPlayer player, Collection<Permission> permissions) {
        // Attempt to replace the users permissions field with new permissions
        try {
            Field field = player.getClass().getDeclaredField("permissions");
            field.setAccessible(true);
            field.set(player, new CaseInsensitiveSet());
            permissions.forEach(p -> player.setPermission(p.getPermission(), p.getValue()));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            getLogger().log(Level.SEVERE, "Failed to set permissions of " + player.getName(), e);
        }

        getProxy().getPluginManager().callEvent(new RecalculatePlayerEvent(player));
    }

    public Collection<Permission> getPlayerPermissions(UUID uuid) throws SQLException {
        try (Connection connection = database.getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "SELECT name " +
                             "FROM memberships " +
                             "JOIN entities " +
                             "ON entities.id = memberships.group_id " +
                             "WHERE member = ?")) {
            ps.setString(1, uuid.toString().replace("-", ""));
            Set<Group> playerGroups = new HashSet<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String group = rs.getString("name");
                    Group g = cachedGroups.get(group);
                    if (g != null) {
                        playerGroups.add(g);
                        g.addPlayer(uuid);
                    }
                }
            }

            if (playerGroups.isEmpty() && cachedGroups.containsKey("member")) {
                Group group = cachedGroups.get("member");
                group.addPlayer(uuid);
                playerGroups.add(group);
            }

            // Clear the players permissions
            Collection<Permission> permissions = new LinkedList<>();
            for (Group g : playerGroups) {
                permissions.addAll(getGroupPermissions(g));
            }

            // Grab the users individual permissions
            try (PreparedStatement psp = connection.prepareStatement(
                    "SELECT permission, value " +
                            "FROM uuidcache " +
                            "JOIN entities " +
                            "ON uuidcache.display_name = entities.display_name " +
                            "JOIN entries " +
                            "ON entries.entity_id = entities.id " +
                            "WHERE uuidcache.uuid = ? " +
                            "AND value = 1")) {
                psp.setString(1, uuid.toString().replace("-", ""));
                try (ResultSet rsp = psp.executeQuery()) {
                    while (rsp.next()) {
                        permissions.add(new Permission(rsp.getString("permission"), rsp.getBoolean("value")));
                    }
                }
            }

            return permissions;
        }
    }
    
    private Set<Permission> getGroupPermissions(Group group) {
        Set<Permission> permissions = new HashSet<>();
        do {
            permissions.addAll(group.getPermissions());
            group = group.getParent();
        } while (group != null);
        return permissions;
    }

    private String getFixedUUID(ProxiedPlayer player) {
        return player.getUniqueId().toString().replace("-", "");
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void preLoginEvent(LoginEvent event) {
        if (event.isCancelled()) {
            return;
        }
        event.registerIntent(this);
        try {
            Collection<Permission> loadedPermissions = getPlayerPermissions(event.getConnection().getUniqueId());
            preLoadedPermissions.put(event.getConnection().getUniqueId(), loadedPermissions);
        } catch (Exception e) {
            getLogger().log(Level.SEVERE, "failed to get permissions of " + event.getConnection().getUniqueId());
            event.setCancelled(true);
            event.setCancelReason("An error occurred when loading your permissions. Please try again later.");
        } finally {
            event.completeIntent(this);
        }
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onPlayerLogin(PostLoginEvent event) {
        ProxiedPlayer player = event.getPlayer();
        Collection<Permission> loaded = preLoadedPermissions.remove(player.getUniqueId());
        updatePlayerPermissions(player, loaded);
    }
    
    @EventHandler
    public void onPlayerDisconnect(PlayerDisconnectEvent event) {
        ProxiedPlayer player = event.getPlayer();
        Collection<Group> groups = playerGroups.removeAll(player);
        groups.forEach(g -> g.removePlayer(player.getUniqueId()));
    }
    
    @EventHandler
    public void onPluginMessage(PluginMessageEvent event) {
        if (!event.getTag().equals("BungeeCord")) {
            return;
        }

        ByteArrayDataInput input = ByteStreams.newDataInput(event.getData());
        String sub = input.readUTF();
        
        if (!sub.equals("zPermissions")) {
            return;
        }
        
        String action = input.readUTF();
        getLogger().info("Handling zPermissions message action: " + action);

        short len = input.readShort();
        byte[] msgbytes = new byte[len];
        input.readFully(msgbytes);

        ByteArrayDataInput in = ByteStreams.newDataInput(msgbytes);
        bungeeMessageHandlers.get(action).accept(in);
    }
}
