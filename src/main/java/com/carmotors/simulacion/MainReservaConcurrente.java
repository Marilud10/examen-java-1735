package com.carmotors.simulacion;

import com.carmotors.config.ConexionFactory;
import com.carmotors.dao.StockInsuficienteException;
import com.carmotors.servicio.taller.ServicioService;
import com.carmotors.servicio.taller.integracion.ReservaRepuesto;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public final class MainReservaConcurrente {

    private MainReservaConcurrente() {
    }

    public static void main(String[] args) throws SQLException, InterruptedException {
        int repuestoId = 1;
        int ordenTecnico1 = 1;
        int ordenTecnico2 = 2;
        int cantidadSolicitada = 1;
        int stockInicial = 1;

        String nombreRepuesto = prepararEscenario(repuestoId, stockInicial);
        int disponibilidadInicial = consultarDisponibilidad(repuestoId);

        System.out.printf("Simulación independiente de reserva concurrente: '%s' (ID %d)%n",
                nombreRepuesto, repuestoId);
        System.out.println("Disponibilidad inicial: " + disponibilidadInicial);
        System.out.println();

        ServicioService servicio = new ServicioService();
        CountDownLatch hilosListos = new CountDownLatch(2);
        CountDownLatch inicioSimultaneo = new CountDownLatch(1);
        AtomicReference<String> reservoUltimaUnidad = new AtomicReference<>("Ninguno");
        AtomicReference<String> reservaRechazada = new AtomicReference<>("Ninguno");

        Thread hilo1 = new Thread(
                crearIntentoReserva(servicio, "Técnico-1", ordenTecnico1, repuestoId, cantidadSolicitada,
                        hilosListos, inicioSimultaneo, reservoUltimaUnidad, reservaRechazada),
                "Hilo-Tecnico-1");

        Thread hilo2 = new Thread(
                crearIntentoReserva(servicio, "Técnico-2", ordenTecnico2, repuestoId, cantidadSolicitada,
                        hilosListos, inicioSimultaneo, reservoUltimaUnidad, reservaRechazada),
                "Hilo-Tecnico-2");

        System.out.println("Técnico-1 intenta reservar 1 unidad para la orden #" + ordenTecnico1 + ".");
        System.out.println("Técnico-2 intenta reservar 1 unidad para la orden #" + ordenTecnico2 + ".");

        hilo1.start();
        hilo2.start();

        hilosListos.await();
        inicioSimultaneo.countDown();

        hilo1.join();
        hilo2.join();

        int disponibilidadFinal = consultarDisponibilidad(repuestoId);

        System.out.println();
        System.out.println("=== Resultado visible ===");
        System.out.println("Técnico que reservó la última unidad: " + reservoUltimaUnidad.get());
        System.out.println("Técnico al que se le rechazó la reserva: " + reservaRechazada.get());
        System.out.println("Disponibilidad final: " + disponibilidadFinal);
    }

    private static Runnable crearIntentoReserva(
            ServicioService servicio,
            String tecnico,
            int ordenId,
            int repuestoId,
            int cantidad,
            CountDownLatch hilosListos,
            CountDownLatch inicioSimultaneo,
            AtomicReference<String> reservoUltimaUnidad,
            AtomicReference<String> reservaRechazada) {
        return () -> {
            hilosListos.countDown();
            try {
                inicioSimultaneo.await();
                servicio.reservarRepuesto(new ReservaRepuesto(ordenId, repuestoId, cantidad));
                reservoUltimaUnidad.set(tecnico + " (Orden #" + ordenId + ")");
                System.out.printf("[%s] Reservó la última unidad para la orden #%d.%n", tecnico, ordenId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.printf("[%s] Ejecución interrumpida.%n", tecnico);
            } catch (StockInsuficienteException | IllegalStateException e) {
                reservaRechazada.set(tecnico + " (Orden #" + ordenId + ")");
                System.out.printf("[%s] Reserva rechazada para la orden #%d: %s%n",
                        tecnico, ordenId, e.getMessage());
            } catch (SQLException e) {
                reservaRechazada.set(tecnico + " (Orden #" + ordenId + ")");
                System.out.printf("[%s] Error de base de datos en orden #%d: %s%n",
                        tecnico, ordenId, e.getMessage());
            }
        };
    }

    private static String prepararEscenario(int repuestoId, int stockDeseado) throws SQLException {
        String nombre = "Repuesto #" + repuestoId;
        try (Connection conexion = ConexionFactory.obtenerConexion()) {
            conexion.setAutoCommit(false);
            try {
                try (PreparedStatement borrarReservas = conexion.prepareStatement(
                        "DELETE FROM servicio_repuestos WHERE repuesto_id = ?")) {
                    borrarReservas.setInt(1, repuestoId);
                    borrarReservas.executeUpdate();
                }
                try (PreparedStatement actualizarStock = conexion.prepareStatement(
                        "UPDATE repuestos SET stock = ? WHERE id = ?")) {
                    actualizarStock.setInt(1, stockDeseado);
                    actualizarStock.setInt(2, repuestoId);
                    int actualizadas = actualizarStock.executeUpdate();
                    if (actualizadas == 0) {
                        throw new IllegalStateException("No existe un repuesto con ID " + repuestoId);
                    }
                }
                try (PreparedStatement buscarNombre = conexion.prepareStatement(
                        "SELECT nombre FROM repuestos WHERE id = ?")) {
                    buscarNombre.setInt(1, repuestoId);
                    try (ResultSet rs = buscarNombre.executeQuery()) {
                        if (rs.next() && rs.getString("nombre") != null && !rs.getString("nombre").isBlank()) {
                            nombre = rs.getString("nombre");
                        }
                    }
                }
                conexion.commit();
            } catch (SQLException | RuntimeException e) {
                conexion.rollback();
                throw e;
            }
        }
        return nombre;
    }

    private static int consultarDisponibilidad(int repuestoId) throws SQLException {
        try (Connection conexion = ConexionFactory.obtenerConexion();
             PreparedStatement ps = conexion.prepareStatement("SELECT stock FROM repuestos WHERE id = ?")) {
            ps.setInt(1, repuestoId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("No existe un repuesto con ID " + repuestoId);
                }
                return rs.getInt("stock");
            }
        }
    }
}

