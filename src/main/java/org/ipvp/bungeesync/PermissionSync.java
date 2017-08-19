package org.ipvp.bungeesync;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import net.md_5.bungee.api.chat.TextComponent;
import net.md_5.bungee.api.connection.ProxiedPlayer;
import net.md_5.bungee.api.event.PostLoginEvent;
import net.md_5.bungee.api.plugin.Listener;
import net.md_5.bungee.api.plugin.Plugin;
import net.md_5.bungee.config.Configuration;
import net.md_5.bungee.config.ConfigurationProvider;
import net.md_5.bungee.config.YamlConfiguration;
import net.md_5.bungee.event.EventHandler;
import net.md_5.bungee.event.EventPriority;
import net.md_5.bungee.util.CaseInsensitiveSet;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Created by Matthew on 21/02/2015.
 */
public class PermissionSync extends Plugin implements Listener {

    private HikariDataSource database;
    private Map<String, Group> cachedGroups;

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
            getLogger().log(Level.SEVERE, "Failed to update group permission cache");
            database.close();
            return;
        }
        getProxy().getPluginManager().registerListener(this, this);
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
                             "ON entities.id = entries.entity_id");
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
                        "ON parent_id = e2.id");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                Group child = cachedGroups.get(rs.getString("child_name"));
                Group parent = cachedGroups.get(rs.getString("parent_name"));
                child.setParent(parent);
            }
        }
    }

    public void synchronizePlayerPermissions(ProxiedPlayer player) throws SQLException {
        try (Connection connection = database.getConnection();
             PreparedStatement ps = connection.prepareStatement(
                     "SELECT name " +
                             "FROM memberships " +
                             "JOIN entities " +
                             "ON entities.id = memberships.group_id " +
                             "WHERE member = ?")) {
            ps.setString(1, getFixedUUID(player));
            Set<Group> playerGroups = new HashSet<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String group = rs.getString("name");
                    Group g = cachedGroups.get(group);
                    if (g != null) {
                        playerGroups.add(g);
                    }
                }
            }

            // Clear the players permissions
            Collection<String> permissions = new CaseInsensitiveSet();
            for (Group g : playerGroups) {
                permissions.addAll(getGroupPermissions(g));
            }

            // Attempt to replace the users permissions field with new permissions
            try {
                Field field = player.getClass().getField("permissions");
                field.setAccessible(true);
                field.set(player, permissions);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                getLogger().log(Level.SEVERE, "Failed to set permissions of " + player.getName(), e);
            }
        }
    }
    
    private Set<String> getGroupPermissions(Group group) {
        Set<String> permissions = new HashSet<>();
        do {
            addGroupPermissions(group, permissions);
            group = group.getParent();
        } while (group != null);
        return permissions;
    }
    
    private void addGroupPermissions(Group group, Set<String> permissions) {
        group.getPermissions().stream()
                .filter(Permission::getValue)
                .map(Permission::getPermission)
                .forEach(permissions::add);
    }

    private String getFixedUUID(ProxiedPlayer player) {
        return player.getUniqueId().toString().replace("-", "");
    }

    @EventHandler(priority = EventPriority.HIGHEST)
    public void onPlayerLogin(PostLoginEvent event) {
        ProxiedPlayer player = event.getPlayer();
        getProxy().getScheduler().runAsync(this, () -> {
            try {
                synchronizePlayerPermissions(player);
            } catch (SQLException e) {
                player.disconnect(TextComponent.fromLegacyText("Failed to synchronize permissions"));
                getLogger().log(Level.SEVERE, "Failed to synchronize permissions of " + player.getName(), e);
            }
        });
    }
}
