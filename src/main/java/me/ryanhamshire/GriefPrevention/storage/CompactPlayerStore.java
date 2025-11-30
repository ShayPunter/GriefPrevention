/*
    GriefPrevention Server Plugin for Minecraft
    Copyright (C) 2012 Ryan Hamshire

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package me.ryanhamshire.GriefPrevention.storage;

import me.ryanhamshire.GriefPrevention.CustomLogEntryTypes;
import me.ryanhamshire.GriefPrevention.GriefPrevention;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compact player data storage that consolidates all player data into a single file
 * instead of individual files per player. This dramatically reduces filesystem
 * overhead on servers with many players.
 *
 * File format: playerdata.dat
 * Each player is stored on a single line: UUID|accruedBlocks|bonusBlocks
 *
 * Players with default values (0,0) are not stored to save space.
 */
public class CompactPlayerStore
{
    // Storage format version for future migrations
    private static final int FORMAT_VERSION = 1;

    // Field separator
    private static final char FIELD_SEP = '|';

    private final Path dataFolder;
    private final boolean useCompression;

    // In-memory cache of all player data
    private final Map<UUID, PlayerBlockData> playerData = new ConcurrentHashMap<>();

    // Track dirty players that need saving
    private final Set<UUID> dirtyPlayers = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean saveInProgress = new AtomicBoolean(false);

    // Group bonus blocks
    private final Map<String, Integer> groupBonusBlocks = new ConcurrentHashMap<>();
    private boolean groupsDirty = false;

    public CompactPlayerStore(Path dataFolder, boolean useCompression)
    {
        this.dataFolder = dataFolder;
        this.useCompression = useCompression;
    }

    /**
     * Initialize the store and load all player data from disk.
     */
    public void initialize() throws IOException
    {
        if (!Files.exists(dataFolder))
        {
            Files.createDirectories(dataFolder);
        }

        loadPlayerData();
        loadGroupData();
    }

    /**
     * Load all player data from the consolidated file.
     */
    private void loadPlayerData() throws IOException
    {
        Path dataFile = getPlayerDataFilePath();

        if (!Files.exists(dataFile))
        {
            GriefPrevention.AddLogEntry("No compact player data file found, starting fresh.");
            return;
        }

        int loadedCount = 0;
        try (BufferedReader reader = createReader(dataFile))
        {
            String line;
            int lineNum = 0;

            // First line is format version
            String versionLine = reader.readLine();
            lineNum++;
            if (versionLine == null || !versionLine.startsWith("V:"))
            {
                throw new IOException("Invalid player data file format: missing version header");
            }
            int version = Integer.parseInt(versionLine.substring(2));

            if (version > FORMAT_VERSION)
            {
                throw new IOException("Player data file version " + version + " is newer than supported version " + FORMAT_VERSION);
            }

            while ((line = reader.readLine()) != null)
            {
                lineNum++;
                if (line.isEmpty() || line.startsWith("#"))
                {
                    continue;
                }

                try
                {
                    PlayerBlockData data = parsePlayerData(line);
                    if (data != null)
                    {
                        playerData.put(data.playerUUID, data);
                        loadedCount++;
                    }
                }
                catch (Exception e)
                {
                    GriefPrevention.AddLogEntry("Error parsing player data at line " + lineNum + ": " + e.getMessage(), CustomLogEntryTypes.Exception);
                }
            }
        }

        GriefPrevention.AddLogEntry("Loaded " + loadedCount + " player records from compact storage.");
    }

    /**
     * Parse a single player data line.
     * Format: UUID|accruedBlocks|bonusBlocks
     */
    private PlayerBlockData parsePlayerData(String line)
    {
        String[] fields = line.split("\\" + FIELD_SEP);

        if (fields.length < 3)
        {
            return null;
        }

        PlayerBlockData data = new PlayerBlockData();
        data.playerUUID = parseCompactUUID(fields[0]);
        data.accruedBlocks = Integer.parseInt(fields[1]);
        data.bonusBlocks = Integer.parseInt(fields[2]);

        return data;
    }

    /**
     * Load group bonus block data.
     */
    private void loadGroupData() throws IOException
    {
        Path groupFile = getGroupDataFilePath();

        if (!Files.exists(groupFile))
        {
            return;
        }

        try (BufferedReader reader = createReader(groupFile))
        {
            String line;

            // First line is format version
            String versionLine = reader.readLine();
            if (versionLine == null || !versionLine.startsWith("V:"))
            {
                throw new IOException("Invalid group data file format: missing version header");
            }

            while ((line = reader.readLine()) != null)
            {
                if (line.isEmpty() || line.startsWith("#"))
                {
                    continue;
                }

                String[] fields = line.split("\\" + FIELD_SEP);
                if (fields.length >= 2)
                {
                    String groupName = fields[0];
                    int blocks = Integer.parseInt(fields[1]);
                    groupBonusBlocks.put(groupName, blocks);
                }
            }
        }

        GriefPrevention.AddLogEntry("Loaded " + groupBonusBlocks.size() + " group bonus block records.");
    }

    /**
     * Get player data by UUID.
     * Returns null if the player has no stored data (default values).
     */
    public PlayerBlockData getPlayerData(UUID playerUUID)
    {
        return playerData.get(playerUUID);
    }

    /**
     * Set player data and mark for saving.
     * If both values are 0, the player is removed from storage.
     */
    public void setPlayerData(UUID playerUUID, int accruedBlocks, int bonusBlocks)
    {
        if (accruedBlocks == 0 && bonusBlocks == 0)
        {
            // Remove players with default values
            if (playerData.remove(playerUUID) != null)
            {
                dirtyPlayers.add(playerUUID);
            }
            return;
        }

        PlayerBlockData data = playerData.computeIfAbsent(playerUUID, k -> new PlayerBlockData());
        data.playerUUID = playerUUID;
        data.accruedBlocks = accruedBlocks;
        data.bonusBlocks = bonusBlocks;

        dirtyPlayers.add(playerUUID);
    }

    /**
     * Get group bonus blocks.
     */
    public int getGroupBonusBlocks(String groupName)
    {
        return groupBonusBlocks.getOrDefault(groupName, 0);
    }

    /**
     * Set group bonus blocks.
     */
    public void setGroupBonusBlocks(String groupName, int blocks)
    {
        if (blocks == 0)
        {
            groupBonusBlocks.remove(groupName);
        }
        else
        {
            groupBonusBlocks.put(groupName, blocks);
        }
        groupsDirty = true;
    }

    /**
     * Get all group bonus blocks.
     */
    public Map<String, Integer> getAllGroupBonusBlocks()
    {
        return Collections.unmodifiableMap(groupBonusBlocks);
    }

    /**
     * Check if there are pending changes.
     */
    public boolean hasPendingChanges()
    {
        return !dirtyPlayers.isEmpty() || groupsDirty;
    }

    /**
     * Save all dirty player data.
     */
    public synchronized void saveAllDirty()
    {
        if (!hasPendingChanges())
        {
            return;
        }

        if (!saveInProgress.compareAndSet(false, true))
        {
            return;
        }

        try
        {
            // For player data, we save the entire file since it's one consolidated file
            if (!dirtyPlayers.isEmpty())
            {
                savePlayerData();
                dirtyPlayers.clear();
            }

            if (groupsDirty)
            {
                saveGroupData();
                groupsDirty = false;
            }
        }
        catch (Exception e)
        {
            GriefPrevention.AddLogEntry("Error saving player data: " + e.getMessage(), CustomLogEntryTypes.Exception);
            e.printStackTrace();
        }
        finally
        {
            saveInProgress.set(false);
        }
    }

    /**
     * Force save all player data.
     */
    public synchronized void saveAll()
    {
        try
        {
            savePlayerData();
            saveGroupData();
            dirtyPlayers.clear();
            groupsDirty = false;
        }
        catch (Exception e)
        {
            GriefPrevention.AddLogEntry("Error saving all player data: " + e.getMessage(), CustomLogEntryTypes.Exception);
            e.printStackTrace();
        }
    }

    /**
     * Save all player data to disk.
     */
    private void savePlayerData() throws IOException
    {
        Path dataFile = getPlayerDataFilePath();
        Path tempFile = dataFile.resolveSibling(dataFile.getFileName() + ".tmp");

        try (BufferedWriter writer = createWriter(tempFile))
        {
            // Write version header
            writer.write("V:" + FORMAT_VERSION);
            writer.newLine();

            // Sort players by UUID for consistent output
            List<PlayerBlockData> sortedData = new ArrayList<>(playerData.values());
            sortedData.sort(Comparator.comparing(d -> d.playerUUID));

            for (PlayerBlockData data : sortedData)
            {
                // Only save non-default values
                if (data.accruedBlocks != 0 || data.bonusBlocks != 0)
                {
                    writer.write(formatPlayerData(data));
                    writer.newLine();
                }
            }
        }

        // Atomic rename
        Files.move(tempFile, dataFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Save group data to disk.
     */
    private void saveGroupData() throws IOException
    {
        Path groupFile = getGroupDataFilePath();
        Path tempFile = groupFile.resolveSibling(groupFile.getFileName() + ".tmp");

        try (BufferedWriter writer = createWriter(tempFile))
        {
            // Write version header
            writer.write("V:" + FORMAT_VERSION);
            writer.newLine();

            // Sort groups by name for consistent output
            List<String> sortedGroups = new ArrayList<>(groupBonusBlocks.keySet());
            Collections.sort(sortedGroups);

            for (String groupName : sortedGroups)
            {
                int blocks = groupBonusBlocks.get(groupName);
                writer.write(groupName + FIELD_SEP + blocks);
                writer.newLine();
            }
        }

        // Atomic rename
        Files.move(tempFile, groupFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Format player data for storage.
     */
    private String formatPlayerData(PlayerBlockData data)
    {
        return formatCompactUUID(data.playerUUID) + FIELD_SEP +
               data.accruedBlocks + FIELD_SEP +
               data.bonusBlocks;
    }

    /**
     * Get all stored player UUIDs.
     */
    public Set<UUID> getAllPlayerUUIDs()
    {
        return Collections.unmodifiableSet(playerData.keySet());
    }

    /**
     * Get total number of stored players.
     */
    public int getPlayerCount()
    {
        return playerData.size();
    }

    private Path getPlayerDataFilePath()
    {
        String extension = useCompression ? ".dat.gz" : ".dat";
        return dataFolder.resolve("playerdata" + extension);
    }

    private Path getGroupDataFilePath()
    {
        String extension = useCompression ? ".dat.gz" : ".dat";
        return dataFolder.resolve("groupdata" + extension);
    }

    private BufferedReader createReader(Path file) throws IOException
    {
        InputStream is = Files.newInputStream(file);
        if (useCompression || file.toString().endsWith(".gz"))
        {
            is = new GZIPInputStream(is);
        }
        return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    }

    private BufferedWriter createWriter(Path file) throws IOException
    {
        OutputStream os = Files.newOutputStream(file);
        if (useCompression)
        {
            os = new GZIPOutputStream(os);
        }
        return new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
    }

    /**
     * Format UUID in compact form (no dashes).
     */
    private String formatCompactUUID(UUID uuid)
    {
        return uuid.toString().replace("-", "");
    }

    /**
     * Parse a compact UUID (with or without dashes).
     */
    private UUID parseCompactUUID(String str)
    {
        if (str.contains("-"))
        {
            return UUID.fromString(str);
        }

        if (str.length() != 32)
        {
            throw new IllegalArgumentException("Invalid compact UUID: " + str);
        }

        String formatted = str.substring(0, 8) + "-" +
                          str.substring(8, 12) + "-" +
                          str.substring(12, 16) + "-" +
                          str.substring(16, 20) + "-" +
                          str.substring(20);
        return UUID.fromString(formatted);
    }

    /**
     * Data class representing stored player data.
     */
    public static class PlayerBlockData
    {
        public UUID playerUUID;
        public int accruedBlocks;
        public int bonusBlocks;
    }
}
